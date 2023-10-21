package org.apache.flink.connector.datadog.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.datadog.source.split.DatadogSplit;
import org.apache.flink.connector.datadog.source.split.DatadogSplitState;

import com.datadog.api.client.ApiClient;
import com.datadog.api.client.v1.model.Event;

import java.util.Map;

public class DatadogSourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<Event, OUT, DatadogSplit, DatadogSplitState> {

    public DatadogSourceReader(
            ApiClient datadogClient,
            RecordEmitter<Event, OUT, DatadogSplitState> recordEmitter,
            SourceReaderContext context,
            Configuration config) {

        super(
                () ->
                        new DatadogSplitReader(
                                datadogClient, (System.currentTimeMillis() - 10000L) / 1000L),
                recordEmitter,
                config,
                context);
    }

    @Override
    public void start() {
        context.sendSplitRequest();
        super.start();
    }

    @Override
    protected void onSplitFinished(Map<String, DatadogSplitState> finishedSplitIds) {
        //        context.sendSplitRequest();
    }

    @Override
    protected DatadogSplitState initializedState(DatadogSplit split) {
        return new DatadogSplitState(System.currentTimeMillis(), split);
    }

    @Override
    protected DatadogSplit toSplitType(String splitId, DatadogSplitState splitState) {
        return splitState.getSplit();
    }
}
