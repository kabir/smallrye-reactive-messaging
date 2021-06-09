package io.smallrye.reactive.messaging.kafka.api;

import java.time.Instant;

import org.apache.kafka.common.header.Headers;

/**
 * For internal use only
 */
class KafkaMessageMetadataImpl<K> implements KafkaMessageMetadata<K> {
    private final String topic;
    private final K recordKey;
    private final int partition;
    private final Instant timestamp;
    private final Headers headers;

    KafkaMessageMetadataImpl(String topic, K recordKey, int partition, Instant timestamp, Headers headers) {
        this.topic = topic;
        this.recordKey = recordKey;
        this.partition = partition;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public K getKey() {
        return recordKey;
    }

    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    @Override
    public int getPartition() {
        return partition;
    }
}
