/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Registers the Splunk bucket connector for ES|QL: query frozen and SmartStore Splunk buckets
 * in place with {@code FROM <dataset>}, decoding {@code rawdata/journal} (gzip, zstd or lz4)
 * on the fly. Schemes: {@code splunkfs://} (shared filesystem) and {@code splunks3://} (S3/MinIO).
 *
 * <p>Requires {@code esql.federation.enabled: true} on every node.
 */
public class SplunkDataSourcePlugin extends Plugin implements DataSourcePlugin {

    private static final Set<String> SCHEMES = Set.of(SplunkConfig.FS_SCHEME, SplunkConfig.S3_SCHEME);

    @Override
    public Set<String> supportedSchemes() {
        return SCHEMES;
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        StorageProviderFactory factory = StorageProviderFactory.noConfigKeys(SplunkStorageProvider::new);
        return Map.of(SplunkConfig.FS_SCHEME, factory, SplunkConfig.S3_SCHEME, factory);
    }

    @Override
    public Set<String> supportedConnectorSchemes() {
        return SCHEMES;
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of(SplunkConfig.TYPE, new SplunkConnectorFactory());
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        DataSourceValidator validator = new SplunkDataSourceValidator();
        return Map.of(validator.type(), validator);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(BucketSplit.ENTRY);
    }
}
