package com.deploy.server.tracking;

import com.aerospike.client.*;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.IspResponse;
import org.cache2k.Cache2kBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@Component
@Scope(value = "singleton")
@RestController
@SuppressWarnings("unused")
public class TrackingServer {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private DatagramSocket mSocket;

    private Thread mSocketThread;

    private static final int DEVICE_TRANSACTION_TTL = 60000;

    private static final int REFRESH_PENDING_REQUESTS_TIMEOUT = 10000;

    private volatile boolean mShutdown = false;

    private static final String COUNTER_KEY = "count";

    private ExecutorService mThreadPool;

    private DatabaseReader mGeoCity;

    private DatabaseReader mGeoISP;

    private DatabaseReader mGeoNET;

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    private AtomicInteger mCPM = new AtomicInteger(0);

    private AerospikeClient mDeviceDB = null;

    private AerospikeClient mCampaignDB = null;

    private synchronized AerospikeClient getDeviceDB(){
        if ( mDeviceDB == null || !mDeviceDB.isConnected()){
            mDeviceDB = new AerospikeClient(config.getDeviceDBHost(), config.getDeviceDBPort());
        }
        return mDeviceDB;
    }

    private synchronized AerospikeClient getCampaignDB(){
        if ( mCampaignDB == null || !mCampaignDB.isConnected()){
            mCampaignDB = new AerospikeClient(config.getCampaignDBHost(), config.getCampaignDBPort());
            RegisterTask counter = mCampaignDB.register(null, getClass().getClassLoader(), "counter.lua", "counter.lua", Language.LUA);
            counter.waitTillComplete();
        }
        return mCampaignDB;
    }

    private interface FilterMatch {
        boolean contains(String value);
    }

    private class PendingRequest {
        private byte[] mID;

        private class Filter {
            private FilterMatch include;
            private FilterMatch exclude;

            public boolean match(String value) {
                if (include != null && !include.contains(value)) {
                    return false;
                }
                if (exclude != null && exclude.contains(value)) {
                    return false;
                }
                return true;
            }
        }

        private class FilterMemoryMatch implements FilterMatch {
            private Set<String> mSet = new HashSet<>();

            public FilterMemoryMatch(List<?> set) {
                for (Object val : set) {
                    if (val instanceof String) {
                        mSet.add((String) val);
                    }
                }
            }

            @Override
            public boolean contains(String value) {
                return mSet.contains(value);
            }
        }


        private class FilterDbSet implements FilterMatch {
            private String mNamespace;
            private String mSetname;

            private FilterDbSet(String namespace, String setname) {
                mNamespace = namespace;
                mSetname = setname;
            }

            public boolean contains(String value) {
                Key key = new Key(mNamespace, mSetname, value);
                return getCampaignDB().exists(null, key);
            }
        }

        private class FilterDbMatch implements FilterMatch {

            private List<FilterDbSet> mSet = new ArrayList<>();

            private FilterDbMatch(List<?> sets) {
                for (Object fset : sets) {
                    Map<?, ?> filter = (Map<?, ?>) fset;
                    String namespace = (String) filter.get("namespace");
                    String setname = (String) filter.get("setname");
                    if (namespace != null && setname != null) {
                        mSet.add(new FilterDbSet(namespace, setname));
                    }
                }
            }

            @Override
            public boolean contains(String value) {
                for (FilterDbSet set : mSet) {
                    if (set.contains(value)) {
                        return true;
                    }
                }
                return false;
            }
        }

        private class Geo {
            Filter city;
            Filter country;
            Filter region;
            Filter isp;
            Filter net;

            private boolean match(InetAddress address) {
                if (city != null ||
                        country != null ||
                        region != null) {
                    try {
                        CityResponse cityResponse = mGeoCity.city(address);
                        if (cityResponse != null) {
                            //System.out.println("Got response " + cityResponse.toJson());
                            if (city != null && !city.match(cityResponse.getCity().getName())) {
                                return false;
                            }
                            if (country != null && !country.match(cityResponse.getCountry().getIsoCode())) {
                                return false;
                            }
                            if (region != null && !region.match(cityResponse.getSubdivisions().get(0).getIsoCode())) {
                                return false;
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Error search city by ip");
                    }
                }
                if (isp != null) {
                    try {
                        IspResponse con = mGeoISP.isp(address);
                        if (con != null) {
                            if (!isp.match(con.getIsp())) {
                                return false;
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Error search ISP by ip");
                    }
                }
                if (net != null) {
                    try {
                        ConnectionTypeResponse con = mGeoNET.connectionType(address);
                        if (con != null) {
                            if (!net.match(con.getConnectionType().name())) {
                                return false;
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Error search NET by ip");
                    }
                }
                return true;
            }
        }

        private Long timestamp;
        private Long counter;
        private String url;
        private Filter customer;
        private Filter id;
        private Filter sn;
        private Filter model;
        private Filter cpu;
        private Filter device;
        private Filter manufacturer;
        private Filter release;
        private Filter sdk;
        private Filter hw;
        private Filter imei;
        private Filter android_id;
        private Filter carrier;
        private Geo geo;

        private FilterMatch getFilterMatch(Object object) {
            if (object instanceof Map) {
                Map<?, ?> filter = (Map<?, ?>) object;
                if (filter.containsKey("type")) {
                    String type = (String) filter.get("type");
                    List<?> data = (List<?>) filter.get("data");
                    if (type != null && data != null) {
                        if (type.equals("set")) {
                            return new FilterDbMatch(data);
                        } else if (type.equals("list")) {
                            return new FilterMemoryMatch(data);
                        }
                    }
                }
            }
            return null;
        }

        private Filter getFilter(String name, Record record) {
            Filter fl = new Filter();
            Map<?, ?> filter = (Map<?, ?>) record.getValue(name);
            if (filter != null) {
                if (filter.containsKey("include")) {
                    fl.include = getFilterMatch(filter.get("include"));
                }
                if (filter.containsKey("exclude")) {
                    fl.exclude = getFilterMatch(filter.get("exclude"));
                }
                if (fl.exclude != null || fl.include != null) {
                    return fl;
                }

            }
            return null;
        }

        public String bytesToHex(byte[] bytes) {
            char[] hexChars = new char[bytes.length * 2];
            for (int j = 0; j < bytes.length; j++) {
                int v = bytes[j] & 0xFF;
                hexChars[j * 2] = hexArray[v >>> 4];
                hexChars[j * 2 + 1] = hexArray[v & 0x0F];
            }
            return new String(hexChars);
        }

        private PendingRequest(Key key, Record record) {
            Object _counter = record.getValue(COUNTER_KEY);
            if (_counter != null) {
                if (_counter instanceof Integer) {
                    counter = Long.valueOf((Integer) _counter);
                } else {
                    counter = (Long) _counter;
                }
            }
            mID = key.digest;
            Object _timestamp = record.getValue("timestamp");
            if (_timestamp != null) {
                if (_timestamp instanceof Integer) {
                    timestamp = Long.valueOf((Integer) _timestamp);
                } else {
                    timestamp = (Long) _timestamp;
                }
            }
            url = (String) record.getValue("url");
            customer = getFilter("customer", record);
            id = getFilter("id", record);
            sn = getFilter("sn", record);
            model = getFilter("model", record);
            cpu = getFilter("cpu", record);
            device = getFilter("device", record);
            manufacturer = getFilter("manufacturer", record);
            release = getFilter("release", record);
            sdk = getFilter("sdk", record);
            hw = getFilter("hw", record);
            imei = getFilter("imei", record);
            android_id = getFilter("android_id", record);
            carrier = getFilter("carrier", record);
            Geo _geo = new Geo();
            _geo.country = getFilter("country", record);
            _geo.region = getFilter("region", record);
            _geo.city = getFilter("city", record);
            _geo.isp = getFilter("isp", record);
            _geo.net = getFilter("net", record);
            if (_geo.country != null ||
                    _geo.region != null ||
                    _geo.city != null ||
                    _geo.isp != null ||
                    _geo.net != null) {
                geo = _geo;
            }
        }

        private boolean isValid(long timestamp) {
            if (timestamp >= this.timestamp) {
                return false;
            }
            return true;
        }

        private boolean match(Filter filter, String value) {
            if (filter != null) {
                return filter.match(value);
            }
            return true;
        }

        private boolean match(String name, Filter filter, String value) {
            boolean res = match(filter, value);
            //System.out.println("match filter " + name + " matched=" + res);
            return res;
        }


        private boolean match(String customerId, InetAddress address, String deviceId, String carrierName, AndroidInfo deviceInfo) {
            if (counter == 0) {
                return false;
            }
            if (!match("customer", customer, customerId) ||
                    !match("id", id, deviceId) ||
                    !match("sn", sn, deviceInfo.sn) ||
                    !match("model", model, deviceInfo.model) ||
                    !match("cpu", cpu, deviceInfo.cpu) ||
                    !match("device", device, deviceInfo.device) ||
                    !match("manufacturer", manufacturer, deviceInfo.manufacturer) ||
                    !match("release", release, deviceInfo.release) ||
                    !match("sdk", sdk, deviceInfo.sdk) ||
                    !match("hw", hw, deviceInfo.hw) ||
                    !match("imei", imei, deviceInfo.imei) ||
                    !match("android_id", android_id, deviceInfo.android_id) ||
                    !match("carrier", carrier, carrierName)
            ) {
                return false;
            }
            if (geo != null) {
                return geo.match(address);
            }
            if (counter > 0) {
                try {
                    Key key = new Key("campaign", mID, "pending", null);
                    Object res = getCampaignDB().execute(null,
                            key,
                            "counter",
                            "decrementWithValidation");
                    // System.out.println("res = " + res);
                    synchronized (this) {
                        Long _counter = 0L;
                        if (res != null) {
                            if (res instanceof Integer) {
                                _counter = Long.valueOf((Integer) res);
                            } else {
                                _counter = (Long) res;
                            }
                        }
                        if ( _counter < counter ){
                            counter = _counter;
                        }
                        if ( _counter > 0 ){
                            return true;
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Exception occurred with counter " + e.getMessage());
                }
            }
            if ( counter == 0 ){
                return false;
            }
            return true;
        }

        private long getTimestamp() {
            return timestamp;
        }

        private String getURL() {
            return url;
        }
    }

    private Lock mLock = new ReentrantLock();

    private List<PendingRequest> mPendingRequests;

    @Autowired
    private TrackingConfig config;

    private static Gson mGson = new GsonBuilder().create();

    private static class RegisterRawResponse {
        public Long expirationTime;
        public byte[] packet;

        public RegisterRawResponse(String packet) {
            //System.out.println("<- " + packet);
            this.packet = packet.getBytes();
            expirationTime = System.currentTimeMillis() + DEVICE_TRANSACTION_TTL;
        }
    }
    private org.cache2k.Cache<UUID, RegisterRawResponse> mRetransmitCache;

    private static class AndroidInfo {
        protected String sn;
        protected String model;
        protected String cpu;
        protected String device;
        protected String manufacturer;
        protected String release;
        protected String sdk;
        protected String hw;
        protected String imei;
        protected String android_id;


        private boolean validate() {
            return sn != null &&
                    model != null &&
                    cpu != null &&
                    device != null &&
                    manufacturer != null &&
                    release != null &&
                    sdk != null &&
                    hw != null &&
                    android_id != null;


        }
    }

    private static class DeviceInfo {
        private String customer;
        private String version;
        private String id;
        private List<String> capabilities = new ArrayList<>();
        private AndroidInfo android = new AndroidInfo();
        private String mccmnc;
        private String carrier;

        private boolean validate() {
            return customer != null && version != null && id != null && android != null && android.validate();
        }
    }

    private static class RegistarData extends AndroidInfo {
        private String customer;
        private String version;
        private String mccmnc;
        private String carrier;
        private String country;
        private String region;
        private String city;
        private String isp;
        private String net;
        private Long created;
        private Long updated;
    }

    private static class RegistarRequest {
        private String id;
        private RegistarData data;
    }

    private void pushRegister(RegistarRequest request) {
        rabbitTemplate.convertAndSend("registar", mGson.toJson(request));
    }


    private static class TrackRequest {
        private UUID id;
        private Long timestamp;
        private DeviceInfo info = new DeviceInfo();

        private boolean validate() {
            return id != null && timestamp != null && info != null && info.validate();
        }

        private String getHash() throws Exception {
            return getMD5(
                    info.customer,
                    info.version,
                    info.android.sn,
                    info.android.model,
                    info.android.cpu,
                    info.android.device,
                    info.android.manufacturer,
                    info.android.release,
                    info.android.sdk,
                    info.android.hw,
                    info.android.imei,
                    info.android.android_id
            );
        }
    }

    private static class TrackResponse {
        private TrackResponse(UUID id, Long timestamp, String request) {
            this.id = id;
            this.timestamp = timestamp;
            this.request = request;
        }

        private UUID id;
        private Long timestamp;
        private String request;
    }

    public static String getMD5(String... strings) throws Exception {
        MessageDigest digest;
        digest = MessageDigest.getInstance("MD5");
        for (String s : strings) {
            if (s != null) {
                digest.update(s.getBytes());
            }
        }
        byte[] md5sum = digest.digest();
        BigInteger bigInt = new BigInteger(1, md5sum);
        String output = bigInt.toString(16);
        return String.format("%32s", output).replace(' ', '0');
    }

    private Bin[] build(Object... objects) {
        ArrayList<Bin> built = new ArrayList<>();
        for (Object b : objects) {
            if (b != null) {
                if (b instanceof Bin) {
                    built.add((Bin) b);
                } else if (b instanceof Collection) {
                    Collection<?> bins = (Collection<?>) b;
                    for (Object bin : bins) {
                        if (bin instanceof Bin) {
                            built.add((Bin) bin);
                        }
                    }
                } else if (b instanceof Bin[]) {
                    Bin[] bins = (Bin[]) b;
                    for (Bin bin : bins) {
                        if (bin != null) {
                            built.add(bin);
                        }
                    }
                }
            }
        }
        return built.toArray(new Bin[built.size()]);
    }

    private abstract class RegisterProcessWorker implements Runnable {

        protected InetAddress mSource;
        protected int mSourcePort;

        private class Geo {
            String country;
            String region;
            String city;
            String isp;
            String net;
        }

        private Geo lookup(InetAddress address) {
            try {
                Geo geo = new Geo();
                CityResponse city = mGeoCity.city(address);
                if (city != null) {
                    geo.country = city.getCountry().getIsoCode();
                    geo.region = city.getSubdivisions().get(0).getIsoCode();
                    geo.city = city.getCity().getName();
                }
                IspResponse isp = mGeoISP.isp(address);
                if (isp != null) {
                    geo.isp = isp.getIsp();
                }
                ConnectionTypeResponse con = mGeoNET.connectionType(address);
                if (isp != null) {
                    geo.net = con.getConnectionType().toString();
                }
                return geo;
            } catch (Exception e) {
                return null;
            }
        }

        private RegisterProcessWorker(InetAddress source,
                                      int sourcePort) {
            mSource = source;
            mSourcePort = sourcePort;

        }

        Bin[] getOnlineStat(Geo geo, TrackRequest request, Bin... bins) {
            return build(
                    bins,
                    new Bin("ip", mSource.getHostAddress()),
                    new Bin("mccmnc", request.info.mccmnc == null ? "UNK" : request.info.mccmnc),
                    new Bin("carrier", request.info.carrier == null ? "UNK" : request.info.carrier),
                    geo == null ? null : build(
                            geo.country == null ? null : new Bin("country", geo.country),
                            geo.region == null ? null : new Bin("region", geo.region),
                            geo.city == null ? null : new Bin("city", geo.city),
                            geo.isp == null ? null : new Bin("isp", geo.isp),
                            geo.net == null ? null : new Bin("net", geo.net)
                    ));
        }

        private boolean refreshOnline(RegistarData data, TrackRequest request) throws Exception {
            WritePolicy wPolicy = new WritePolicy();
            wPolicy.expiration = config.getTTL();
            boolean registar = false;
            try {
                Key onlineKey = new Key("device", "online-" + request.info.customer, request.info.id);

                Record rec = getDeviceDB().get(null, onlineKey);
                if (rec == null) {
                    Geo geo = lookup(mSource);
                    getDeviceDB().put(wPolicy, onlineKey,
                            getOnlineStat(geo, request, new Bin("id", request.info.id),
                                    new Bin("created", System.currentTimeMillis()),
                                    new Bin("touch", System.currentTimeMillis()))
                    );
                    if (geo != null) {
                        data.country = geo.country;
                        data.region = geo.region;
                        data.city = geo.city;
                        data.isp = geo.isp;
                        data.net = geo.net;
                        registar = true;
                    }

                } else {
                    wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
                    boolean do_update = false;
                    Geo geo = null;
                    String ip = (String) rec.getValue("ip");
                    if (ip == null || !ip.equals(mSource.getHostAddress())) {
                        do_update = true;
                        geo = lookup(mSource);
                        if (geo != null) {
                            data.country = geo.country;
                            data.region = geo.region;
                            data.city = geo.city;
                            data.isp = geo.isp;
                            data.net = geo.net;
                            registar = true;
                        }
                    }

                    String mccmnc = (String) rec.getValue("mccmnc");
                    if (request.info.mccmnc != null &&
                            !request.info.mccmnc.equals(mccmnc)) {
                        do_update = true;
                        data.mccmnc = request.info.mccmnc;
                        data.carrier = request.info.carrier;
                        registar = true;
                    }
                    if (do_update) {
                        getDeviceDB().put(wPolicy, onlineKey,
                                getOnlineStat(geo, request, new Bin("touch", System.currentTimeMillis()))
                        );
                    } else {
                        getDeviceDB().put(wPolicy, onlineKey, new Bin("touch", System.currentTimeMillis()));
                    }
                }
            } catch (Exception e) {
                throw e;
            }
            return registar;
        }

        Bin[] getDeviceStat(Long cts, TrackRequest request, Bin... bins) throws Exception {
            return build(
                    bins,
                    new Bin("updated", cts),
                    new Bin("customer", request.info.customer),
                    new Bin("version", request.info.version),
                    new Bin("android_id", request.info.android.android_id),
                    new Bin("cpu", request.info.android.cpu),
                    new Bin("device", request.info.android.device),
                    new Bin("hw", request.info.android.hw),
                    new Bin("imei", request.info.android.imei),
                    new Bin("model", request.info.android.model),
                    new Bin("manufacturer", request.info.android.manufacturer),
                    new Bin("sdk", request.info.android.sdk),
                    new Bin("release", request.info.android.release),
                    new Bin("sn", request.info.android.sn),
                    new Bin("hash", request.getHash()));
        }


        private <T> T compareAndSet(Record rec, String name, T value) {
            T current = (T) rec.getValue(name);
            if (current == null ||
                    value == null) {
                return value;
            }
            if (!current.equals(value)) {
                return value;
            }
            return null;
        }

        protected TrackResponse processRequest(TrackRequest request) {
            String req = null;
            Long timestamp = request.timestamp;
            try {
                RegistarRequest registarRequest = new RegistarRequest();
                registarRequest.data = new RegistarData();
                registarRequest.id = request.info.id;
                boolean sendRegistar = false;
                //System.out.println("reg " + request.info.id);
                Key registerKey = new Key("device", "register", request.info.id);
                Long cts = System.currentTimeMillis();
                registarRequest.data.updated = cts;

                if (!getDeviceDB().exists(null, registerKey)) {
                    WritePolicy wPolicy = new WritePolicy();
                    wPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                    getDeviceDB().put(wPolicy, registerKey,
                            getDeviceStat(cts,
                                    request,
                                    new Bin("id", request.info.id),
                                    new Bin("created", cts)
                            )
                    );
                    sendRegistar = true;
                    //System.out.println("1");
                    registarRequest.data.created = cts;
                    registarRequest.data.customer = request.info.customer;
                    registarRequest.data.version = request.info.version;
                    registarRequest.data.sn = request.info.android.sn;
                    registarRequest.data.model = request.info.android.model;
                    registarRequest.data.cpu = request.info.android.cpu;
                    registarRequest.data.device = request.info.android.device;
                    registarRequest.data.manufacturer = request.info.android.manufacturer;
                    registarRequest.data.release = request.info.android.release;
                    registarRequest.data.sdk = request.info.android.sdk;
                    registarRequest.data.hw = request.info.android.hw;
                    registarRequest.data.imei = request.info.android.imei;
                    registarRequest.data.android_id = request.info.android.android_id;

                } else {
                    Record registerRec = getDeviceDB().get(null, registerKey);
                    String hash = (String) registerRec.getValue("hash");
                    if (!request.getHash().equals(hash)) {
                        sendRegistar = true;
                        //System.out.println("2");
                        WritePolicy wPolicy = new WritePolicy();
                        wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
                        getDeviceDB().put(wPolicy, registerKey,
                                getDeviceStat(cts, request));
                        registarRequest.data.customer = compareAndSet(registerRec, "customer", request.info.customer);
                        registarRequest.data.version = compareAndSet(registerRec, "version", request.info.version);
                        registarRequest.data.sn = compareAndSet(registerRec, "sn", request.info.android.sn);
                        registarRequest.data.model = compareAndSet(registerRec, "model", request.info.android.model);
                        registarRequest.data.cpu = compareAndSet(registerRec, "cpu", request.info.android.cpu);
                        registarRequest.data.device = compareAndSet(registerRec, "device", request.info.android.device);
                        registarRequest.data.manufacturer = compareAndSet(registerRec, "manufacturer", request.info.android.manufacturer);
                        registarRequest.data.release = compareAndSet(registerRec, "release", request.info.android.release);
                        registarRequest.data.sdk = compareAndSet(registerRec, "sdk", request.info.android.sdk);
                        registarRequest.data.hw = compareAndSet(registerRec, "hw", request.info.android.hw);
                        registarRequest.data.imei = compareAndSet(registerRec, "imei", request.info.android.imei);
                        registarRequest.data.android_id = compareAndSet(registerRec, "android_id", request.info.android.android_id);
                    }
                }
                if (refreshOnline(registarRequest.data, request)) {
                    sendRegistar = true;
                    //System.out.println("3");
                }
                if (sendRegistar) {
                    pushRegister(registarRequest);
                }
                if ( mCPM.get() < config.getCPM()) {
                    List<PendingRequest> pendingRequests;
                    mLock.lock();
                    pendingRequests = mPendingRequests;
                    mLock.unlock();

                    if (pendingRequests != null) {
                        for (PendingRequest pending : mPendingRequests) {
                            if (pending.isValid(timestamp)) {
                                if (pending.match(request.info.customer, mSource, request.info.id, request.info.carrier, request.info.android)) {
                                    if ( mCPM.updateAndGet(value -> value < config.getCPM() ? value + 1 : config.getCPM()) < config.getCPM()) {
                                        req = pending.getURL();
                                        timestamp = pending.getTimestamp();
                                        //System.out.println(request.info.id + " -> " + req);
                                        break;
                                    }else{
                                        break;
                                    }

                                }else{
                                    timestamp = pending.getTimestamp();
                                }
                            }
                        }
                    }
                }

            } catch (Exception e) {
                System.out.println("Exception " + e.getMessage());
                e.printStackTrace();
            }
            return new TrackResponse(request.id, timestamp, req);
        }
    }

    private class RegisterUDPProcessWorker extends RegisterProcessWorker {
        private byte[] mPacket;
        private int mPacketSize;
        private DatagramSocket mSocket;

        private RegisterUDPProcessWorker(byte[] data, int data_size, DatagramSocket socket, InetAddress source, int sourcePort) {
            super(source, sourcePort);
            mPacket = data.clone();
            mPacketSize = data_size;
            mSocket = socket;
        }

        protected void sendResponse(TrackResponse response) {
            RegisterRawResponse rawResponse = new RegisterRawResponse(mGson.toJson(response));
            mRetransmitCache.put(response.id, rawResponse);
            sendResponse(rawResponse);
        }

        private void sendResponse(RegisterRawResponse response) {
            try {
                //System.out.println("<- " + mSource.getHostAddress() + ":" + mSourcePort);
                mSocket.send(new DatagramPacket(response.packet, response.packet.length, mSource, mSourcePort));
            } catch (IOException e) {
                System.out.println("IO exception " + e.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                JsonElement request = new JsonParser().parse(new JsonReader(new InputStreamReader(new ByteArrayInputStream(mPacket, 0, mPacketSize))));
                //System.out.println(request.toString());
                TrackRequest reg_request = mGson.fromJson(request, TrackRequest.class);
                if (reg_request.validate()) {
                    RegisterRawResponse response = mRetransmitCache.get(reg_request.id);
                    if (response != null) {
                        sendResponse(response);
                    } else {
                        sendResponse(processRequest(reg_request));
                    }
                }
            } catch (Exception e) {
                System.out.println("Could not process request e: " + e.getMessage() + " packet: " + new String(mPacket, 0, mPacketSize));
            }
        }
    }

    private class RegisterRESTProcessWorker extends RegisterProcessWorker {
        private String mData;
        DeferredResult<String> mDeferredResult;

        private RegisterRESTProcessWorker(String request, DeferredResult<String> deferredResult, InetAddress source, int sourcePort) {
            super(source, sourcePort);
            mData = request;
            mDeferredResult = deferredResult;
        }

        @Override
        public void run() {
            try {
                JsonElement request = new JsonParser().parse(mData);
                TrackRequest reg_request = mGson.fromJson(request, TrackRequest.class);
                if (reg_request.validate()) {
                    mDeferredResult.setResult(mGson.toJson(processRequest(reg_request)));
                }else{
                    mDeferredResult.setErrorResult("Invalid request");
                }
            } catch (Exception e) {
                System.out.println("Could not process request e: " + e.getMessage() + " packet: " + mData);
                mDeferredResult.setErrorResult(e.getMessage());
            }
        }
    }


    @PostConstruct
    private void init() throws Exception {

        mRetransmitCache = Cache2kBuilder.of(UUID.class, RegisterRawResponse.class)
                .name("retransmit")
                .expireAfterWrite(DEVICE_TRANSACTION_TTL, TimeUnit.MILLISECONDS)
                .sharpExpiry(false)
                .entryCapacity(100000)
                .permitNullValues(false)
                .disableStatistics(true)
                .build();

        System.out.println("Loading GeoIP2-City.mmdb");
        mGeoCity = new DatabaseReader.Builder(getClass().getClassLoader().getResourceAsStream("GeoIP2-City.mmdb")).build();
        System.out.println("Loading GeoIP2-ISP.mmdb");
        mGeoISP = new DatabaseReader.Builder(getClass().getClassLoader().getResourceAsStream("GeoIP2-ISP.mmdb")).build();
        System.out.println("Loading GeoIP2-Connection-Type.mmdb");
        mGeoNET = new DatabaseReader.Builder(getClass().getClassLoader().getResourceAsStream("GeoIP2-Connection-Type.mmdb")).build();
        System.out.println("Device Aerospike: " + config.getDeviceDBHost() + ":" + config.getDeviceDBPort());
        getDeviceDB();
        System.out.println("Campaign Aerospike: " + config.getCampaignDBHost() + ":" + config.getCampaignDBPort());
        getCampaignDB();

        System.out.println("Processing pool size " + config.getPoolSize());
        mThreadPool = Executors.newFixedThreadPool(config.getPoolSize());
        System.out.println("Starting UDP server on port " + config.getTrackingPort());
        mSocket = new DatagramSocket(config.getTrackingPort());
        mSocketThread = new Thread(() -> {
            try {
                byte[] receiveData = new byte[1024];
                while (!mShutdown) {
                    DatagramPacket packet = new DatagramPacket(receiveData, receiveData.length);
                    try {
                        mSocket.receive(packet);
                        
                        mThreadPool.execute(new RegisterUDPProcessWorker(packet.getData(), packet.getLength(), mSocket, packet.getAddress(), packet.getPort()));
                    } catch (SocketTimeoutException e) {
                        //do nothing, it seems no pings and timeout appeared
                    }
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        });
        mSocketThread.start();

    }

    @PreDestroy
    private void finit() throws Exception {
        mShutdown = true;
        mSocketThread.join();
    }

    @Scheduled(fixedRate = 60000)
    private void resetCPM(){
        int cpm = mCPM.get();
        System.out.println("CPM is " + cpm);
        mCPM.set(0);
    }


    @Scheduled(fixedRate = REFRESH_PENDING_REQUESTS_TIMEOUT)
    private void refresh() {
        System.out.println("retransmitCache " + mRetransmitCache.toString());
        List<PendingRequest> pendingRequests = new ArrayList<>();
        try {
            getCampaignDB().scanAll(null, "campaign", "pending", (Key key, Record record) -> {
                        if (pendingRequests.size() == 0) {
                            System.out.println("=====Reading requests=====");
                        }
                        PendingRequest request = new PendingRequest(key, record);

                        //request.dump();
                        pendingRequests.add(request);
                    }
            );
            pendingRequests.sort(Comparator.comparingLong(PendingRequest::getTimestamp));
            System.out.println("Reloaded pending requests " + pendingRequests.size());
            mLock.lock();
            mPendingRequests = pendingRequests;
            mLock.unlock();
        } catch (Exception e) {
            System.out.println("Couldn't load pending requests from database " + e.getMessage());
        }
    }

    @RequestMapping(value = "/track", method = RequestMethod.POST, produces = "application/json")
    public DeferredResult<String> get(@RequestBody String args, HttpServletRequest request) {
        DeferredResult<String> deferredResult = new DeferredResult<>();
        try {
            System.out.println("REST");
            mThreadPool.execute(new RegisterRESTProcessWorker(args, deferredResult, InetAddress.getByName(request.getRemoteAddr()), request.getRemotePort()));
        }catch (Exception e){
            deferredResult.setResult(mGson.toJson(e.getMessage()));
        }
        return deferredResult;
    }
}
