package com.deploy.server.tracking;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TrackingConfig {
    private static int mTrackingPort = 9500;

    public static Integer getTrackingPort(){
        return mTrackingPort;
    }

    @Value("${POOL.size}")
    private int mPoolSize;

    public int getPoolSize(){
        return mPoolSize;
    }

    @Value("${DEVICE.DB.host}")
    private String mDeviceDBHost;

    public String getDeviceDBHost(){
        return mDeviceDBHost;
    }

    @Value("${DEVICE.DB.port}")
    private int mDeviceDBPort;

    public int getDeviceDBPort(){
        return mDeviceDBPort;
    }


    @Value("${CAMPAIGN.DB.host}")
    private String mCampaignDBHost;

    public String getCampaignDBHost(){
        return mCampaignDBHost;
    }

    @Value("${CAMPAIGN.DB.port}")
    private int mCampaignDBPort;

    public int getCampaignDBPort(){
        return mCampaignDBPort;
    }

    @Value("${TTL}")
    private int mTTL;
    public int getTTL() { return mTTL; }

    @Value("${CPM}")
    private int mCPM;
    public int getCPM() { return mCPM; }
}
