package com.juliodev.ProductsMicroservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

import static com.juliodev.ProductsMicroservice.constants.ProductMicroserviceConstants.TOPIC_NAME;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic createTopic(){
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(3).replicas(3)
                .configs(Map.of("min.insync.replicas", "2"))
                .build();
    }
}
