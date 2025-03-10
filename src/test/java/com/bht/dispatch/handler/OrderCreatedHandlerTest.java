package com.bht.dispatch.handler;

import com.bht.dispatch.message.OrderCreated;
import com.bht.dispatch.service.DispatcherService;
import com.bht.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.*;


class OrderCreatedHandlerTest {

    private OrderCreatedHandler orderCreatedHandler;
    private DispatcherService dispatcherServiceMock;

    @BeforeEach
    void setUp() {
        dispatcherServiceMock = mock(DispatcherService.class);
        orderCreatedHandler = new OrderCreatedHandler(dispatcherServiceMock);
    }

    @Test
    void listen_Success() throws ExecutionException, InterruptedException {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        orderCreatedHandler.listen(0, key, testEvent);
        verify(dispatcherServiceMock, times(1)).process(key, testEvent);
    }

    @Test
    void listen_ServiceThrowsException() throws ExecutionException, InterruptedException {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Service failure")).when(dispatcherServiceMock).process(key, testEvent);
        orderCreatedHandler.listen(0, key, testEvent);
        verify(dispatcherServiceMock, times(1)).process(key, testEvent);

    }
}