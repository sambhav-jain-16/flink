package org.apache.flink.connector.datadog.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

public class DatadogSplit implements SourceSplit {

    private final Long startingTime;

    public DatadogSplit(Long startingTime) {
        this.startingTime = startingTime;
    }

    public Long getStartingTime() {
        return startingTime;
    }

    @Override
    public String splitId() {
        return "0";
    }
}
