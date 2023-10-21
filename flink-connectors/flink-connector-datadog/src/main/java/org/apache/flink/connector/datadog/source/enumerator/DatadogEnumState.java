package org.apache.flink.connector.datadog.source.enumerator;

import org.apache.flink.connector.datadog.source.split.DatadogSplit;

import java.util.Collection;

public class DatadogEnumState {
    private final Collection<DatadogSplit> splits;

    public DatadogEnumState(Collection<DatadogSplit> splits) {
        this.splits = splits;
    }
}
