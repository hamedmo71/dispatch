package com.bht.dispatch.service;

import com.bht.dispatch.message.OrderCreated;
import org.springframework.stereotype.Service;

@Service
public class DispatcherService {

    public void process(OrderCreated payload){
        // no-op
    }
}
