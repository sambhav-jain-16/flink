package org.apache.flink.connector.datadog.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.datadog.source.deserializer.DatadogDeserializationSchema;
import org.apache.flink.connector.datadog.source.split.DatadogSplitState;
import org.apache.flink.util.Collector;

import com.datadog.api.client.v1.model.Event;

public class DatadogRecordEmitter<OUT> implements RecordEmitter<Event, OUT, DatadogSplitState> {

    private final DatadogDeserializationSchema<OUT> deserializationSchema;
    private final SourceOutputWrapper<OUT> sourceOutputWrapper;

    public DatadogRecordEmitter(DatadogDeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        this.sourceOutputWrapper = new SourceOutputWrapper<OUT>();
    }

    @Override
    public void emitRecord(Event data, SourceOutput<OUT> output, DatadogSplitState splitState)
            throws Exception {
        splitState.setTimestamp(data.getDateHappened());
        sourceOutputWrapper.setSourceOutput(output);
        deserializationSchema.deserialize(data, sourceOutputWrapper);
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {
        private SourceOutput<T> sourceOutput;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record);
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }
    }
}
