package com.bht.dispatch.util;

import com.bht.dispatch.message.OrderCreated;

import java.util.UUID;

public class TestEventData {
    public static OrderCreated buildOrderCreatedEvent(UUID orderId, String item){
        return OrderCreated.builder()
                .orderId(orderId)
                .item(item)
                .build();
    }
}
