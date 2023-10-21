package org.apache.flink.connector.datadog.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.datadog.source.split.DatadogSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

public class DatadogSplitEnumerator implements SplitEnumerator<DatadogSplit, DatadogEnumState> {

    private final SplitEnumeratorContext<DatadogSplit> context;
    private final Queue<DatadogSplit> remainingSplits;

    public DatadogSplitEnumerator(
            SplitEnumeratorContext<DatadogSplit> context,
            Collection<DatadogSplit> remainingSplits) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>(remainingSplits);
    }

    @Override
    public void start() {
        // nothing to start
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final DatadogSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<DatadogSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        context.assignSplit(new DatadogSplit(System.currentTimeMillis() / 1000L), subtaskId);
    }

    @Override
    public DatadogEnumState snapshotState(long checkpointId) throws Exception {
        return new DatadogEnumState(remainingSplits);
    }

    @Override
    public void close() throws IOException {}
}
