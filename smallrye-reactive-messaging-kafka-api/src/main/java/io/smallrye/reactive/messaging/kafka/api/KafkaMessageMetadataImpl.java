package io.smallrye.reactive.messaging.kafka.api;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * For internal use only
 */
public class KafkaMessageMetadataImpl<K> implements KafkaMessageMetadata<K> {
    private final String topic;
    private final K recordKey;
    private final int partition;
    private final Instant timestamp;
    private final Headers headers;

    protected KafkaMessageMetadataImpl(String topic, K recordKey, int partition, Instant timestamp, Headers headers) {
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

    protected static abstract class ExtensibleBuilder<K, B extends ExtensibleBuilder<K, B, M>, M extends KafkaMessageMetadata<K>>
            implements KafkaMessageMetadata.Builder<K> {
        private String topic;
        private K recordKey;
        private int partition = -1;
        private Instant timestamp = null;
        private Headers headers;

        @Override
        public B withTopic(String topic) {
            this.topic = topic;
            return getThis();
        }

        @Override
        public B withKey(K recordKey) {
            this.recordKey = recordKey;
            return getThis();
        }

        @Override
        public B withPartition(int partition) {
            this.partition = partition;
            return getThis();
        }

        @Override
        public B withTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return getThis();
        }

        @Override
        public B withHeaders(Headers headers) {
            this.headers = headers;
            return getThis();
        }

        @Override
        public B withHeaders(List<RecordHeader> headers) {
            List<Header> iterable = new ArrayList<>(headers);
            return withHeaders(new RecordHeaders(iterable));
        }

        protected String getTopic() {
            return topic;
        }

        protected K getRecordKey() {
            return recordKey;
        }

        protected int getPartition() {
            return partition;
        }

        protected Instant getTimestamp() {
            return timestamp;
        }

        protected Headers getHeaders() {
            return headers;
        }

        protected abstract B getThis();

        public abstract M build();
    }

    static class DefaultBuilder<K> extends ExtensibleBuilder<K, DefaultBuilder<K>, KafkaMessageMetadataImpl<K>> {
        @Override
        protected DefaultBuilder<K> getThis() {
            return this;
        }

        @Override
        public KafkaMessageMetadataImpl build() {
            return new KafkaMessageMetadataImpl(getTopic(), getRecordKey(), getPartition(), getTimestamp(), getHeaders());
        }
    }
}
