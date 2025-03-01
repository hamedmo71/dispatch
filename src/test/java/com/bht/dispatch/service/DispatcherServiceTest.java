package com.bht.dispatch.service;

import com.bht.dispatch.message.OrderCreated;
import com.bht.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.*;

class DispatcherServiceTest {

    private DispatcherService dispatcherService;
    @BeforeEach
    void setUp() {
        dispatcherService = new DispatcherService();
    }

    @Test
    void process() {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        dispatcherService.process(testEvent);
    }
}