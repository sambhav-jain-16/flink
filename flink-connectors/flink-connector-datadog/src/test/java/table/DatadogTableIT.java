package table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

public class DatadogTableIT extends AbstractTestBase {

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    @Test
    public void testTable() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        env.setParallelism(1);

        final String createTable =
                String.format(
                        "create table datadog (\n"
                                + "  host STRING,\n"
                                + "  id STRING,\n"
                                + "  url STRING,\n"
                                + "  title STRING\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'api.key' = '%s',\n"
                                + "  'app.key' = '%s'\n"
                                + ")",
                        "datadog",
                        "",
                        ""
                );

        tEnv.executeSql(createTable);

        // ---------- Consume stream from Datadog -------------------

        String query = "SELECT\n" + "  url ,\n" + "  title \n" + "FROM datadog WHERE title LIKE "
                + "'%[TEST]%'\n";
        DataStream<Row> result =
                tEnv.toDataStream(tEnv.sqlQuery(query), Row.class);
        System.out.println("Printing the following rows " + result.print());
        env.execute("Datadog record print");
    }
}
