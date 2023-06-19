package com.deploy.server.tracking;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Collections;

@EnableScheduling
@SpringBootApplication
public class TrackingMain {
    public static void main(String[] args) throws Exception {
        SpringApplication app = new SpringApplication(TrackingMain.class);
        app.setDefaultProperties(Collections
                .singletonMap("server.port", TrackingConfig.getTrackingPort()+1));
        app.run(args);
    }
}
