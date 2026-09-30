/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * CRUD-time validator for the {@code splunk} data source type. Registering it is what makes
 * {@code PUT /_query/data_source} accept {@code "type": "splunk"}.
 */
final class SplunkDataSourceValidator implements DataSourceValidator {

    /** Anything settable on the data source is also settable (and overridable) per dataset. */
    private static final Set<String> KEYS;
    static {
        Set<String> keys = new LinkedHashSet<>(SplunkConfig.S3_KEYS);
        keys.addAll(SplunkConfig.READ_KEYS);
        KEYS = Set.copyOf(keys);
    }

    // As with the ClickHouse connector on 9.5.4: secret=true settings reach connectors still
    // encrypted, so nothing is marked secret. Put credentials on the dataset (see README).
    private static final Set<String> SECRET_KEYS = Set.of();

    @Override
    public String type() {
        return SplunkConfig.TYPE;
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
        if (datasourceSettings == null || datasourceSettings.isEmpty()) {
            return Map.of();
        }
        ValidationException errors = new ValidationException();
        Map<String, DataSourceSetting> out = new HashMap<>();
        for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
            if (KEYS.contains(e.getKey()) == false) {
                errors.addValidationError("unknown setting [" + e.getKey() + "] for data source type [splunk]; recognised: " + KEYS);
                continue;
            }
            out.put(e.getKey(), new DataSourceSetting(e.getValue(), SECRET_KEYS.contains(e.getKey())));
        }
        if (errors.validationErrors().isEmpty() == false) {
            throw errors;
        }
        return out;
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        ValidationException errors = new ValidationException();
        if (SplunkConfig.handles(resource) == false) {
            errors.addValidationError(
                "dataset resource must be splunkfs:///path or splunks3://bucket/prefix, got [" + resource + "]"
            );
        }
        if (datasetSettings != null) {
            for (String key : datasetSettings.keySet()) {
                if (KEYS.contains(key) == false) {
                    errors.addValidationError("unknown dataset setting [" + key + "] for data source type [splunk]; recognised: " + KEYS);
                }
            }
        }
        if (errors.validationErrors().isEmpty()) {
            try {
                SplunkConfig.parse(resource, datasetSettings == null ? Map.of() : datasetSettings);
            } catch (IllegalArgumentException e) {
                errors.addValidationError(e.getMessage());
            }
        }
        if (errors.validationErrors().isEmpty() == false) {
            throw errors;
        }
        return datasetSettings == null ? Map.of() : Map.copyOf(datasetSettings);
    }
}
