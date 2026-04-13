package com.juliodev.ProductsMicroservice.service;

import com.juliodev.ProductsMicroservice.rest.CreateProductRestModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.juliodev.ProductsMicroservice.constants.ProductMicroserviceConstants.TOPIC_NAME;

@Service
public class ProductServiceImpl implements ProductService{
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductRestModel productRestModel) {
        String productId = UUID.randomUUID().toString();
        //TODO:Persist product details into a database table before publishing the event
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
                productId,
                productRestModel.getTitle(),
                productRestModel.getPrice(),
                productRestModel.getQuantity());
        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate
                .send(TOPIC_NAME,productId, productCreatedEvent);

        future.whenComplete((result, exception)->{
            if(exception != null){
                LOGGER.error("***** Failed to send the message:" + exception.getMessage());
            } else{
                LOGGER.info("***** Message send successfully" + result.getRecordMetadata());
            }
        });

        LOGGER.info("***** Returning Product ID");
        return productId;
    }
}
