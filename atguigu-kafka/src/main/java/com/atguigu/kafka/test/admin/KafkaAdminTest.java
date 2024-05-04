package com.atguigu.kafka.test.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;

/**
 * 创建主题测试
 */

public class KafkaAdminTest {
    public static void main(String[] args) {
        // 配置对象
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 创建管理者对象
        final Admin admin = Admin.create(configMap);
        // 主题名称（支持字母、数字。注意：点、下划线、中横线也支持，但不建议同时使用）
        String topicName = "test1";
        // 分区梳理
        int partitionCount = 1;
        // 副本数量
        short replicationCount = 1;
        NewTopic topic1 = new NewTopic(topicName, partitionCount, replicationCount);

        // 支持自己分配副本方案
        String topicName2 = "test2";
        Map<Integer, List<Integer>> map = new HashMap<>();
        map.put(0, Arrays.asList(3, 1)); // 分区0的Leader broker_id=3，剩下的是follower
        map.put(1, Arrays.asList(2, 3));
        map.put(2, Arrays.asList(1, 2));
        NewTopic topic2 = new NewTopic(topicName2, map);

        final CreateTopicsResult topics = admin.createTopics(
                Arrays.asList(topic1, topic2)
        );
        // 关闭管理者对象
        admin.close();
    }
}
