package io.smallrye.reactive.messaging.kafka.api;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;

public interface KafkaMessageMetadata<K> {

    String getTopic();

    K getKey();

    Instant getTimestamp();

    Headers getHeaders();

    int getPartition();

    interface Builder<K> {
        Builder<K> withTopic(String topic);
        Builder<K> withKey(K recordKey);
        Builder<K> withPartition(int partition);
        Builder<K> withTimestamp(Instant timestamp);
        Builder<K> withHeaders(Headers headers);
        Builder<K> withHeaders(List<RecordHeader> headers);
        KafkaMessageMetadata<K> build();
    }

    static <K> Builder<K> builder() {
        return new Builder<K>() {
            private String topic;
            private K recordKey;
            private int partition = -1;
            private Instant timestamp = null;
            private Headers headers;

            @Override
            public Builder<K> withTopic(String topic) {
                this.topic = topic;
                return null;
            }

            @Override
            public Builder<K> withKey(K recordKey) {
                this.recordKey = recordKey;
                return this;
            }

            @Override
            public Builder<K> withPartition(int partition) {
                this.partition = partition;
                return this;
            }

            @Override
            public Builder<K> withTimestamp(Instant timestamp) {
                this.timestamp = timestamp;
                return this;
            }

            @Override
            public Builder<K> withHeaders(Headers headers) {
                this.headers = headers;
                return this;
            }

            @Override
            public Builder<K> withHeaders(List<RecordHeader> headers) {
                List<Header> iterable = new ArrayList<>(headers);
                return withHeaders(new RecordHeaders(iterable));
            }

            @Override
            public KafkaMessageMetadata<K> build() {
                return new KafkaMessageMetadataImpl<>(topic, recordKey, partition, timestamp, headers);
            }
        };
    }
}
