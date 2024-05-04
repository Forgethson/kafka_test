package com.atguigu.kafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerTest {
    public static void main(String[] args) {
        // 配置属性集合
        Map<String, Object> configMap = new HashMap<>();
        // 配置属性：Kafka服务器集群地址
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 配置属性：Kafka生产的数据为KV对，所以在生产数据进行传输前需要分别对K,V进行对应的序列化操作
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 创建Kafka生产者对象，建立Kafka连接
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);
        // 准备数据：构造对象时需要传递 【Topic主题名称】，【Key】，【Value】三个参数
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "key1", "value1");
        // 生产（发送）数据
        producer.send(record);
        // 关闭生产者连接
        producer.close();
    }
}