package org.apache.flink.connector.datadog.table.deseralizer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datadog.source.deserializer.DatadogDeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.datadog.api.client.v1.model.Event;

import java.io.IOException;

import static java.lang.String.format;

public class DatadogRowDataDeserializationSchema implements DatadogDeserializationSchema<RowData> {

    /** Type information describing the result type. */
    private final TypeInformation<RowData> typeInfo;

    private final JsonToRowDataConverters.JsonToRowDataConverter converter;

    public DatadogRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> typeInfo) {
        this.typeInfo = typeInfo;
        this.converter =
                new JsonToRowDataConverters(false, true, TimestampFormat.SQL)
                        .createConverter(rowType);
    }

    @Override
    public RowData deserialize(Event document) throws Exception {

        if (document == null) {
            return null;
        }
        try {
            ObjectMapper mapper = new ObjectMapper();
            return (RowData) converter.convert(mapper.convertValue(document, JsonNode.class));
        } catch (Throwable t) {
            throw new IOException(
                    format("Failed to deserialize JSON '%s'.", document.toString()), t);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }
}
