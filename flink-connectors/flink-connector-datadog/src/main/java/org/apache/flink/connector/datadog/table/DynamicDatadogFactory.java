package org.apache.flink.connector.datadog.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.datadog.table.DatadogConnectionOptions.API_KEY;
import static org.apache.flink.connector.datadog.table.DatadogConnectionOptions.APP_KEY;

public class DynamicDatadogFactory implements DynamicTableSourceFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig options = helper.getOptions();
        helper.validate();

        return new DynamicDatadogTableSource(
                options.get(API_KEY), options.get(APP_KEY), context.getPhysicalRowDataType());
    }

    @Override
    public String factoryIdentifier() {
        return "datadog";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(API_KEY);
        requiredOptions.add(APP_KEY);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return ImmutableSet.of();
    }
}
