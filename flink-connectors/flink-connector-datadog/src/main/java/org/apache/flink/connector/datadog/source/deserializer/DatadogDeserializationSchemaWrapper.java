package org.apache.flink.connector.datadog.source.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.datadog.api.client.v1.model.Event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Objects;
import java.util.Optional;

public class DatadogDeserializationSchemaWrapper implements DatadogDeserializationSchema<Event> {

    private static final long serialVersionUID = 8629926836484133131L;

    public DatadogDeserializationSchemaWrapper() throws Exception {}

    @Override
    public Event deserialize(Event document) throws Exception {
        return document;
    }

    private byte[] writeBytes(Event document) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutputStream out = null;
            out = new ObjectOutputStream(bos);
            out.writeChars(Objects.requireNonNull(document.getAlertType()).getValue());
            out.writeLong(
                    Optional.ofNullable(document.getDateHappened())
                            .orElse(System.currentTimeMillis() / 1000L));
            out.writeChars(Optional.ofNullable(document.getDeviceName()).orElse(""));
            out.writeChars(Optional.ofNullable(document.getHost()).orElse(""));
            out.writeLong(Optional.ofNullable(document.getId()).orElse(0L));
            out.writeChars(Optional.ofNullable(document.getIdStr()).orElse(""));
            out.writeChars(Optional.ofNullable(document.getPayload()).orElse(""));
            out.writeChars(Objects.requireNonNull(document.getPriority()).getValue());
            out.writeChars(Optional.ofNullable(document.getSourceTypeName()).orElse(""));
            //        out.writeObject(Objects.requireNonNull(document.getTags()).toArray());
            out.writeChars(Optional.ofNullable(document.getText()).orElse(""));
            out.writeChars(Optional.ofNullable(document.getTitle()).orElse(""));
            out.writeChars(Optional.ofNullable(document.getUrl()).orElse(""));
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
