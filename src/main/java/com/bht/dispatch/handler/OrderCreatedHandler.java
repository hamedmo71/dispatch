package com.bht.dispatch.handler;

import com.bht.dispatch.message.OrderCreated;
import com.bht.dispatch.message.OrderUpdated;
import com.bht.dispatch.service.DispatcherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
@Component
@KafkaListener(
        id = "OrderConsumerClient",
        topics = "order.created",
        groupId = "dispatch.order.created.consumer",
        containerFactory = "kafkaListenerContainerFactory"
)
public class OrderCreatedHandler {

    private final DispatcherService dispatcherService;

    @KafkaHandler
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload) {
        log.info("Received Message: partition: {} - key: {} -Payload: {}", partition, key, payload);
        try {
            dispatcherService.process(key, payload);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Processing failure", e);
        }
    }

    @KafkaHandler
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderUpdated payload) {
        log.info("Received Message: partition: {} - key: {} -Payload: {}. this payload is for update", partition, key, payload);
        try {
            dispatcherService.process(key, payload);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Processing failure", e);
        }
    }
}
