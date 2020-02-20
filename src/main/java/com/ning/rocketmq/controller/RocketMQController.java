package com.ning.rocketmq.controller;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("rocket")
public class RocketMQController {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    public static Map<String, DefaultMQPushConsumer> consumerMap = new ConcurrentHashMap<>();

    /**
     * NameServer地址
     */
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    private  DefaultMQProducer producer;

    @PostConstruct
    public void init() throws MQClientException {
        producer=new DefaultMQProducer("jay_producer_group");
        producer.setNamesrvAddr("127.0.0.1:9876"); //它会从命名服务器上拿到broker的地址
        producer.start();
    }

    @RequestMapping("hello")
    public String helloworld(String topic,String tag) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        int num=0;
        while(num<3){
            num++;
            //Topic
            //tags -> 标签 （分类） -> (筛选)
            Message message=new Message(topic,tag,("Hello , RocketMQ:"+num).getBytes());

            //消息路由策略
            producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    return list.get(0);
                }
            },"key-"+num);
        }
        return "OK";
    }


    @RequestMapping("consumer")
    public String consumer(String topic,String consumerGroup,String tag){
        createConsumer(topic,consumerGroup, Thread.currentThread().getName(),tag);
        return "create consumer success";
    }

    @RequestMapping("cancelConsumer")
    public String cancelConsumer(String consumerGroup){
        DefaultMQPushConsumer consumer = consumerMap.get(consumerGroup);
        consumer.shutdown();
        return "cancel consumer success";
    }

    private void createConsumer(String topic, String consumerGroup, String threadName,String tag) {
        System.out.println("创建消费者的线程threadName： [" + threadName + "]");
        //消费者的组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        //指定NameServer地址，多个地址以 ; 隔开
        consumer.setNamesrvAddr(namesrvAddr);
        try {
            //订阅PushTopic下Tag为push的消息
            consumer.subscribe(topic, tag);
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
//                                throw new Exception("消费失败，稍后重试");
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
            consumerMap.put(consumerGroup, consumer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
