package org.apache.flink.connector.datadog.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class DatadogConnectionOptions {

    public DatadogConnectionOptions() {}

    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key("api.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies api key.");

    public static final ConfigOption<String> APP_KEY =
            ConfigOptions.key("app.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies app key.");
}
