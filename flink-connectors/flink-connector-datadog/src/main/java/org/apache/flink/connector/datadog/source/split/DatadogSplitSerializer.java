package org.apache.flink.connector.datadog.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DatadogSplitSerializer implements SimpleVersionedSerializer<DatadogSplit> {

    private static final int CURRENT_VERSION = 0;

    public DatadogSplitSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(DatadogSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(split.getStartingTime());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DatadogSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            Long timestamp = in.readLong();
            return new DatadogSplit(timestamp);
        }
    }
}
