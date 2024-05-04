package com.atguigu.kafka.test.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 * 1. 实现 ProducerInterceptor 接口
 * 2. 定义 KV 泛型
 * 3. 重写方法
 * *         onSend
 * *         onAcknowledgement
 * *         close
 * *         configure
 */
public class ValueInterceptor implements ProducerInterceptor<String, String> {
    @Override
    // 发送数据时调用
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<>(producerRecord.topic(), producerRecord.key(), producerRecord.value() + "~添加拦截器标志~");
    }

    @Override
    // 发送完毕后，收到服务器响应时调用
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // recordMetadata的toString方法：this.topicPartition.toString() + "@" + this.offset
        System.out.println(recordMetadata);
    }

    @Override
    // 生产者对象关闭时调用
    public void close() {
        System.out.println("Producer close");
    }

    @Override
    // 创建生产者对象时调用
    public void configure(Map<String, ?> map) {
        System.out.println("configure map=" + map);
    }
}
