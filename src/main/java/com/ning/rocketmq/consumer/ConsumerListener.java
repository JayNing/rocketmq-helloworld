package com.ning.rocketmq.consumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

/**
 * author JayNing
 * created by 2020/4/9 10:37
 **/
@Service
//@RocketMQMessageListener(topic = "ningjianjian", consumerGroup = "ningGp01")
public class ConsumerListener implements RocketMQListener<String> {
    private Logger logger = LoggerFactory.getLogger(ConsumerListener.class);
    @Override
    public void onMessage(String message) {
        logger.info("RocketMQMessageListener receive : {}" , message);
    }
}
