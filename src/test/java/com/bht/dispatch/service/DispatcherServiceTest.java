package com.bht.dispatch.service;

import com.bht.dispatch.message.OrderCreated;
import com.bht.dispatch.message.OrderDispatched;
import com.bht.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DispatcherServiceTest {

    private DispatcherService dispatcherService;
    private KafkaTemplate kafkaProducerMock;

    @BeforeEach
    void setUp() {
        kafkaProducerMock = mock(KafkaTemplate.class);
        dispatcherService = new DispatcherService(kafkaProducerMock);
    }

    @Test
    void process_Success() throws ExecutionException, InterruptedException {
        String key = randomUUID().toString();
        when(kafkaProducerMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        dispatcherService.process(key, testEvent);
        verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
    }

    @Test
    void process_ProducerThrowsException() throws ExecutionException, InterruptedException {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Producer failure")).when(kafkaProducerMock).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

        RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> dispatcherService.process(key, testEvent));

        verify(kafkaProducerMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        assertThat(runtimeException.getMessage(), equalTo("Producer failure"));
    }
}