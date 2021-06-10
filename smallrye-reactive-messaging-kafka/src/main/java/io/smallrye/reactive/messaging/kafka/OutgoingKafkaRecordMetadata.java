package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;

import org.apache.kafka.common.header.Headers;

import io.smallrye.reactive.messaging.kafka.api.KafkaMessageMetadataImpl;

public class OutgoingKafkaRecordMetadata<K> extends KafkaMessageMetadataImpl<K> {

    public static <K> OutgoingKafkaRecordMetadataBuilder<K> builder() {
        return new OutgoingKafkaRecordMetadataBuilder<>();
    }

    public OutgoingKafkaRecordMetadata(String topic, K key, int partition, Instant timestamp,
            Headers headers) {
        super(topic, key, partition, timestamp, headers);
    }

    public static final class OutgoingKafkaRecordMetadataBuilder<K>
            extends ExtensibleBuilder<K, OutgoingKafkaRecordMetadataBuilder<K>, OutgoingKafkaRecordMetadata<K>> {
        @Override
        protected OutgoingKafkaRecordMetadataBuilder<K> getThis() {
            return this;
        }

        public OutgoingKafkaRecordMetadata<K> build() {
            return new OutgoingKafkaRecordMetadata<>(
                    getTopic(),
                    getRecordKey(),
                    getPartition(),
                    getTimestamp(),
                    getHeaders());
        }
    }
}
