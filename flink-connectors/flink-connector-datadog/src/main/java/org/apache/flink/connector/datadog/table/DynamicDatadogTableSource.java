package org.apache.flink.connector.datadog.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datadog.source.DatadogSource;
import org.apache.flink.connector.datadog.source.deserializer.DatadogDeserializationSchema;
import org.apache.flink.connector.datadog.table.deseralizer.DatadogRowDataDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class DynamicDatadogTableSource implements ScanTableSource {

    private final String apiKey;
    private final String appKey;
    private final DataType producedDataType;
    private int limit = -1;

    public DynamicDatadogTableSource(String apiKey, String appKey, DataType producedDataType) {
        this.apiKey = apiKey;
        this.appKey = appKey;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        return new DynamicDatadogTableSource(apiKey, appKey, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Datadog";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> typeInfo =
                runtimeProviderContext.createTypeInformation(producedDataType);

        final DatadogDeserializationSchema<RowData> deserializationSchema =
                new DatadogRowDataDeserializationSchema(rowType, typeInfo);

        DatadogSource<RowData> source = new DatadogSource<>(apiKey, appKey, deserializationSchema);

        return SourceProvider.of(source);
    }
}
