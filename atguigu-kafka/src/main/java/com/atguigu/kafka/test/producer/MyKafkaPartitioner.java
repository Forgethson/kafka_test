package com.atguigu.kafka.test.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器实现步骤：
 * 1. 实现Partitioner接口
 * 2. 重写方法
 * *   partition : 返回分区编号，从0开始
 * *   close
 * *   configure
 */

public class MyKafkaPartitioner implements Partitioner {
    /**
     * 分区算法 - 根据业务自行定义即可
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     * @return 分区编号，从0开始
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 自定义分区算法...
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
