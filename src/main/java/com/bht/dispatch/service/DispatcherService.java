package com.bht.dispatch.service;

import com.bht.dispatch.client.StockServiceClient;
import com.bht.dispatch.message.OrderCreated;
import com.bht.dispatch.message.OrderDispatched;
import com.bht.dispatch.message.OrderUpdated;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class DispatcherService {

    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";

    private final KafkaTemplate<String, Object> kafkaProducer;

    private static final UUID APPLICATION_ID = UUID.randomUUID();

    public final StockServiceClient stockServiceClient;


    public void process(String key, OrderCreated orderCreated) throws ExecutionException, InterruptedException {

        String available = stockServiceClient.checkAvailability(orderCreated.getItem());
        if (Boolean.valueOf(available)){
            OrderDispatched orderDispatched = OrderDispatched.builder()
                    .orderId(orderCreated.getOrderId())
                    .processedById(APPLICATION_ID)
                    .note("Dispatched: " + orderCreated.getItem())
                    .build();
            kafkaProducer.send(ORDER_DISPATCHED_TOPIC, key, orderDispatched).get();

            log.info("Sent message: key: {} -orderId: {} - processedByID: {}", key, orderCreated.getOrderId(), APPLICATION_ID);
        } else {
            log.info("Item {} is not available!", orderCreated.getItem());
        }

    }

    public void process(String key, OrderUpdated orderUpdated) throws ExecutionException, InterruptedException {
        OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderUpdated.getOrderId())
                .processedById(APPLICATION_ID)
                .note("UpdateDispatched: " + orderUpdated.getItem())
                .build();
        kafkaProducer.send(ORDER_DISPATCHED_TOPIC, key, orderDispatched).get();

        log.info("Sent message: key: {} -orderId: {} - processedByID: {}", key, orderUpdated.getOrderId(), APPLICATION_ID);
    }
}
