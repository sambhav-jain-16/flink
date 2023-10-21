package org.apache.flink.connector.datadog.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.datadog.source.deserializer.DatadogDeserializationSchema;
import org.apache.flink.connector.datadog.source.enumerator.DatadogEnumState;
import org.apache.flink.connector.datadog.source.enumerator.DatadogSplitEnumerator;
import org.apache.flink.connector.datadog.source.reader.DatadogRecordEmitter;
import org.apache.flink.connector.datadog.source.reader.DatadogSourceReader;
import org.apache.flink.connector.datadog.source.split.DatadogSplit;
import org.apache.flink.connector.datadog.source.split.DatadogSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.datadog.api.client.ApiClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class DatadogSource<OUT>
        implements Source<OUT, DatadogSplit, DatadogEnumState>, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = -6205055126994055336L;
    private final String apiKey;
    private final String appKey;
    private final DatadogDeserializationSchema<OUT> deserializationSchema;

    public DatadogSource(
            String apiKey, String appkey, DatadogDeserializationSchema<OUT> deserializationSchema) {
        this.apiKey = apiKey;
        this.appKey = appkey;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public SplitEnumerator<DatadogSplit, DatadogEnumState> createEnumerator(
            SplitEnumeratorContext<DatadogSplit> enumContext) throws Exception {
        final List<DatadogSplit> splits =
                Collections.singletonList(new DatadogSplit(System.currentTimeMillis()/1000L));
        return new DatadogSplitEnumerator(enumContext, splits);
    }

    @Override
    public SplitEnumerator<DatadogSplit, DatadogEnumState> restoreEnumerator(
            SplitEnumeratorContext<DatadogSplit> enumContext, DatadogEnumState checkpoint)
            throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<DatadogSplit> getSplitSerializer() {
        return new DatadogSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DatadogEnumState> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<OUT, DatadogSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        HashMap<String, String> secrets = new HashMap<>();
        secrets.put("apiKeyAuth", apiKey);
        secrets.put("appKeyAuth", appKey);

        ApiClient datadogClient = ApiClient.getDefaultApiClient();
        datadogClient.configureApiKeys(secrets);

        DatadogRecordEmitter<OUT> emitter = new DatadogRecordEmitter<>(deserializationSchema);

        return new DatadogSourceReader<>(
                datadogClient,
                emitter,
                readerContext,
                readerContext.getConfiguration());
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
