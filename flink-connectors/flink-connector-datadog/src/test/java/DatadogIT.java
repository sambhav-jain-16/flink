import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.datadog.source.DatadogSource;
import org.apache.flink.connector.datadog.source.deserializer.DatadogDeserializationSchemaWrapper;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.UserCodeClassLoader;

import com.datadog.api.client.v1.model.Event;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Objects;

public class DatadogIT {

    @Test
    public void testBoundedDatadogSource() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DatadogSource<Event> source =
                new DatadogSource<>(
                        "",
                        "",
                        new DatadogDeserializationSchemaWrapper());

        DataStream<Event> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "datadog " + "Source");

        DataStream<String> records =
                stream.filter(
                                (record) ->
                                        Objects.equals(
                                                Objects.requireNonNull(record.getAlertType())
                                                        .toString(),
                                                "error"))
                        .map(Event::getTitle);

        records.print().name("Test");
        env.execute("Test");
    }

    @NotNull
    private static JsonDeserializationSchema<Event> getObjectNodeJsonDeserializationSchema() {
        JsonDeserializationSchema<Event> jsonFormat = new JsonDeserializationSchema<>(Event.class);
        jsonFormat.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return new UnregisteredMetricsGroup();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return (UserCodeClassLoader) Thread.currentThread().getContextClassLoader();
                    }
                });
        return jsonFormat;
    }
}
