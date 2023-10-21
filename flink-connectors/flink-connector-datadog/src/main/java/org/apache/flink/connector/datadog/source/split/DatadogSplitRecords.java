package org.apache.flink.connector.datadog.source.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import com.datadog.api.client.v1.model.Event;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

public class DatadogSplitRecords implements RecordsWithSplitIds<Event> {

    Long previousTimeStamp;
    List<Event> records;
    Long index;

    boolean finish;

    public DatadogSplitRecords(Long previousTimeStamp, List<Event> records, boolean finishSplit) {
        this.previousTimeStamp = previousTimeStamp;
        this.records = records;
        this.index = 0L;
        this.finish = finishSplit;
    }

    @Nullable
    @Override
    public String nextSplit() {
        return index >= records.size() ? null : "0";
    }

    @Nullable
    @Override
    public Event nextRecordFromSplit() {
        if (index >= records.size()) {

            return null;
        }

        return records.get(Math.toIntExact(index++));
    }

    @Override
    public Set<String> finishedSplits() {
        return finish ? ImmutableSet.of("0") : ImmutableSet.of();
    }
}
