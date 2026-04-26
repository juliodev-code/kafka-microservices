package com.juliodev.ProductsMicroservice.service;

import com.juliodev.ProductsMicroservice.rest.CreateProductRestModel;
import com.juliodev.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;

import static com.juliodev.ProductsMicroservice.constants.ProductMicroserviceConstants.TOPIC_NAME;

@Service
public class ProductServiceImpl implements ProductService{
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductRestModel productRestModel) throws Exception {
        String productId = UUID.randomUUID().toString();
        //TODO:Persist product details into a database table before publishing the event
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
                productId,
                productRestModel.getTitle(),
                productRestModel.getPrice(),
                productRestModel.getQuantity());

        LOGGER.info("***** Before publishing a ProductCreatedEvent");

        SendResult<String, ProductCreatedEvent> result = kafkaTemplate
                .send(TOPIC_NAME,productId, productCreatedEvent).get();

        LOGGER.info("Partition: " + result.getRecordMetadata().partition());
        LOGGER.info("Topic: " + result.getRecordMetadata().topic());
        LOGGER.info("Offset: " + result.getRecordMetadata().offset());

        LOGGER.info("***** Returning Product ID");
        return productId;
    }
}
