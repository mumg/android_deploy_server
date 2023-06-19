package com.deploy.server.tracking;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TrackingMQ {

    @Value("${MQ.host}")
    private String mMQConfHost;

    @Value("${MQ.username}")
    private String mMQConfUsername;

    @Value("${MQ.password}")
    private String mMQConfPassword;


    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory =
                new CachingConnectionFactory(mMQConfHost);
        connectionFactory.setUsername(mMQConfUsername);
        connectionFactory.setPassword(mMQConfPassword);
        return connectionFactory;
    }

    @Bean
    public RabbitTemplate rabbitTemplate() {
        return new RabbitTemplate(connectionFactory());
    }
}
