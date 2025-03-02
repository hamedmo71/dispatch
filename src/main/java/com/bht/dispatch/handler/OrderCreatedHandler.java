package com.bht.dispatch.handler;

import com.bht.dispatch.message.OrderCreated;
import com.bht.dispatch.service.DispatcherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

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
        dispatcherService.process(payload);
    }
}
