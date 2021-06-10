package io.smallrye.reactive.messaging.kafka.api;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.microprofile.reactive.messaging.Message;

public interface KafkaMessageMetadata<K> {

    String getTopic();

    K getKey();

    Instant getTimestamp();

    Headers getHeaders();

    int getPartition();

    static <K> Builder<K> builder() {
        return new KafkaMessageMetadataImpl.DefaultBuilder<K>();
    }

    static <K> KafkaMessageMetadata<K> read(Message<?> message) {
        // TODO...
        return null;
    }

    static <K> Builder<K> read(Class<K> key, Message<?> message) {
        // TODO...
        return null;
    }

    static <T> Message<T> set(Message<T> message) {
        // TODO...
        return null;
    }

    interface Builder<K> {
        Builder<K> withTopic(String topic);

        Builder<K> withKey(K recordKey);

        Builder<K> withPartition(int partition);

        Builder<K> withTimestamp(Instant timestamp);

        Builder<K> withHeaders(Headers headers);

        Builder<K> withHeaders(List<RecordHeader> headers);

        KafkaMessageMetadata<K> build();
    }

}
