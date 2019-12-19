package com.ning.rocketmq.controller;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("rocket")
public class RocketMQController {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    private Executor executor
            = new ThreadPoolExecutor(3,3,10, TimeUnit.SECONDS,new BlockingArrayQueue<>());

    /**
     * NameServer地址
     */
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    @RequestMapping("hello")
    public String helloworld(String topic,String msg){
        //如果向topic指定的tag下发送消息，格式：topic:tag
        rocketMQTemplate.syncSend(topic,msg);
        return "OK";
    }


    @RequestMapping("consumer")
    public String consumer(String topic,String consumerGroup){
        executor.execute(() -> {
            createConsumer(topic,consumerGroup, Thread.currentThread().getName());
        });

        return "create consumer success";
    }

    private void createConsumer(String topic, String consumerGroup, String threadName) {
        System.out.println("创建消费者的线程threadName： [" + threadName + "]");
        //消费者的组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        //指定NameServer地址，多个地址以 ; 隔开
        consumer.setNamesrvAddr(namesrvAddr);
        try {
            //订阅PushTopic下Tag为push的消息
            consumer.subscribe(topic, "*");
            //设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
            //如果非第一次启动，那么按照上次消费的位置继续消费
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

            //记录消费失败重试次数
            int failRetryCount = 3;
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                    MessageExt ext = null;
                    try {
                        for (MessageExt messageExt : list) {
                            ext = messageExt;
//                            if (messageExt.getReconsumeTimes() == failRetryCount){
//                                System.out.println("失败重试次数已达到" + (failRetryCount - 1) + "次");
//                                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                            }else{

                                System.out.println("线程threadName： [" + threadName + "] , Consumer [" + consumerGroup + "] messageExt: " + messageExt);//输出消息内容

                                String messageBody = new String(messageExt.getBody(), "utf-8");

                                System.out.println("线程threadName： [" + threadName + "] , Consumer [" + consumerGroup + "]  消费响应：Msg: " + messageExt.getMsgId() + ",msgBody: " + messageBody);//输出消息内容
                                throw new Exception("消费失败，稍后重试");
//                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("try{}catch()到了消费失败的异常。。。。。。");
                        //exception的情况，一般重复16次 10s、30s、1mins、2mins、3mins等
                        if (ext.getReconsumeTimes() == failRetryCount){
                            System.out.println("失败重试次数已达到" + failRetryCount + "次");
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }

                        return ConsumeConcurrentlyStatus.RECONSUME_LATER; //稍后再试
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //消费成功
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
