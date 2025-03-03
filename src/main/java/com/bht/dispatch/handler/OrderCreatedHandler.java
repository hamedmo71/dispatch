package com.bht.dispatch.handler;

import com.bht.dispatch.message.OrderCreated;
import com.bht.dispatch.service.DispatcherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderCreatedHandler {

    private final DispatcherService dispatcherService;

    @KafkaListener(
            id = "OrderConsumerClient",
            topics = "order.created",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(OrderCreated payload){
        log.info("Received Message: Payload: {}", payload);
        try {
            dispatcherService.process(payload);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Processing failure", e);
        }
    }
}
