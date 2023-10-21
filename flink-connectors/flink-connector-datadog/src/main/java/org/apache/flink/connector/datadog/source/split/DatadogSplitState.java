package org.apache.flink.connector.datadog.source.split;

public class DatadogSplitState {

    private Long timestamp;
    DatadogSplit split;

    public DatadogSplitState(Long timestamp, DatadogSplit split) {
        this.timestamp = timestamp;
        this.split = split;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public DatadogSplit getSplit() {
        return split;
    }
}
