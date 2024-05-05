package com.atguigu.kafka.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaConsumerOffsetTest {
    public static void main(String[] args) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configMap.put("group.id", "CG_wjd_test1_08");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap);
        String topic = "test1";
        consumer.subscribe(Collections.singletonList(topic));

        boolean flg = true;
        while (flg) {
            // 拉取数据，获取基本集群信息
            consumer.poll(Duration.ofMillis(100));
            // 根据集群的基本信息配置需要消费的主题及偏移量
            final Set<TopicPartition> assignment = consumer.assignment();
            if (assignment != null && !assignment.isEmpty()) {
                for (TopicPartition topicPartition : assignment) {
                    if (topic.equals(topicPartition.topic())) {
                        consumer.seek(topicPartition, 120);
                        flg = false;
                    }
                }
            }
        }
        // 消费数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("K = " + record.key() + ", V = " + record.value());
            }
//            consumer.commitSync();
//            consumer.commitAsync();
        }
    }
}