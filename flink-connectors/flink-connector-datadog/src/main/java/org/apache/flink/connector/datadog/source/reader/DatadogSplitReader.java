package org.apache.flink.connector.datadog.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.datadog.source.split.DatadogSplit;
import org.apache.flink.connector.datadog.source.split.DatadogSplitRecords;

import com.datadog.api.client.ApiClient;
import com.datadog.api.client.ApiException;
import com.datadog.api.client.v1.api.EventsApi;
import com.datadog.api.client.v1.model.Event;
import com.datadog.api.client.v1.model.EventListResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DatadogSplitReader implements SplitReader<Event, DatadogSplit> {

    private final EventsApi eventsApi;
    Long previousTimeStamp;

    List<DatadogSplit> splits;

    public DatadogSplitReader(ApiClient apiClient, Long startTime) {
        this.eventsApi = new EventsApi(apiClient);
        this.previousTimeStamp = startTime;
    }

    @Override
    public RecordsWithSplitIds<Event> fetch() throws IOException {

        try {
            Long endTimestamp = System.currentTimeMillis() / 1000L;
            if (previousTimeStamp >= endTimestamp) {
                Thread.sleep(1000L);
                endTimestamp = System.currentTimeMillis() / 1000L;
            }
            EventListResponse result = eventsApi.listEvents(previousTimeStamp, endTimestamp);
            DatadogSplitRecords data =
                    new DatadogSplitRecords(previousTimeStamp, result.getEvents(), false);
            if (!Objects.requireNonNull(result.getEvents()).isEmpty()) {
                Long nextTimeStamp =
                        Optional.ofNullable(result.getEvents().get(0).getDateHappened())
                                .orElse(previousTimeStamp);
                previousTimeStamp =
                        nextTimeStamp <= previousTimeStamp
                                ? Instant.ofEpochSecond(previousTimeStamp)
                                        .plusSeconds(1)
                                        .getEpochSecond()
                                : nextTimeStamp;
            }
            return data;
        } catch (ApiException e) {
            System.err.println("Exception when calling EventsApi#listEvents");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();

            return new DatadogSplitRecords(previousTimeStamp, null, true);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DatadogSplit> splitsChanges) {
        // do nothing
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        // do Nothing
    }
}
