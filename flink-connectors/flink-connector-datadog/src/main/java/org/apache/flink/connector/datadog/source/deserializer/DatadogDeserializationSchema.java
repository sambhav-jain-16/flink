package org.apache.flink.connector.datadog.source.deserializer;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.datadog.api.client.v1.model.Event;

import java.io.Serializable;

public interface DatadogDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
    T deserialize(Event document) throws Exception;

    default void deserialize(Event document, Collector<T> out) throws Exception {
        T deserialize = deserialize(document);
        if (deserialize != null) {
            out.collect(deserialize);
        }
    }
}
