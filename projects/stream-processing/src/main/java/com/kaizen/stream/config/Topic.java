package com.kaizen.stream.config;

/**
 * Single place for topic facts; must match deploy/create_kafka_topics.sh (checked by TopicTest).
 * Nơi duy nhất khai báo topic; phải khớp deploy/create_kafka_topics.sh (TopicTest kiểm tra).
 */
public enum Topic {
    ORDER_BUNDLES("order-bundles", "order", 6),
    PAYMENT_BUNDLES("payment-bundles", "payment", 6),
    INVENTORY_MOVEMENTS("inventory-movements", "inventory", 6),
    LOGISTICS_EVENTS("logistics-events", "logistics", 3);

    private final String topicName;
    private final String shortName;
    private final int partitions;

    Topic(String topicName, String shortName, int partitions) {
        this.topicName = topicName;
        this.shortName = shortName;
        this.partitions = partitions;
    }

    public String topicName() {
        return topicName;
    }

    /** Used in consumer group names. / Dùng trong tên consumer group. */
    public String shortName() {
        return shortName;
    }

    public int partitions() {
        return partitions;
    }

    /** Kafka topic that receives events of this topic that failed parsing or validation. / Topic nhận sự kiện lỗi. */
    public String deadLetterTopicName() {
        return topicName + ".dlq";
    }
}
