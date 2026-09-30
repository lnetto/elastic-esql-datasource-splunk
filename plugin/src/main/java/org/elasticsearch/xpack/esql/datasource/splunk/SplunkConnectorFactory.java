/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Factory for Splunk bucket connectors ({@code splunkfs://} and {@code splunks3://}).
 *
 * <p>Metadata resolution lists the store once (proving the location is reachable and holds
 * buckets) and, with {@code fields: auto}, decodes a small sample to find the indexed fields.
 * Split discovery lists again, prunes buckets against the query's {@code _time}/{@code index}
 * filters ({@link FilterHints}) and hands out one {@link BucketSplit} per surviving bucket.
 */
final class SplunkConnectorFactory implements ConnectorFactory {

    private static final Logger logger = LogManager.getLogger(SplunkConnectorFactory.class);

    @Override
    public String type() {
        return SplunkConfig.TYPE;
    }

    @Override
    public boolean canHandle(String location) {
        return SplunkConfig.handles(location);
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        ConfigKeyValidator.check(SplunkConfig.effective(config), List.of(SplunkConfig.CONFIG_KEYS));
        SplunkConfig.parse(location, config);
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> rawConfig) {
        SplunkConfig config = SplunkConfig.parse(location, rawConfig);
        int buckets;
        Set<String> fields = new LinkedHashSet<>(config.fields());
        try (BucketStore store = config.openStore()) {
            List<BucketDiscovery.BucketRef> found = BucketDiscovery.discover(store, config);
            buckets = found.size();
            if (config.autoFields()) {
                fields.addAll(discoveredFields(location, rawConfig, store, found));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to list Splunk buckets at [" + location + "]: " + e.getMessage(), e);
        }
        if (buckets == 0 && config.earliest() == BucketDiscovery.UNKNOWN && config.latest() == BucketDiscovery.UNKNOWN) {
            String hint = config.scheme().equals(SplunkConfig.FS_SCHEME) ? " (splunkfs roots must be readable and under path.repo)" : "";
            throw new IllegalStateException("No Splunk buckets (rawdata/journal*) found at [" + location + "]" + hint);
        }
        // sourceType must be the URI scheme: the operator/split registries key connectors by scheme.
        return new SimpleSourceMetadata(
            SplunkSchema.attributes(List.copyOf(fields), config.timestampField()),
            config.scheme(),
            location,
            null,
            null,
            Map.of("buckets", buckets),
            SplunkConfig.resolved(location, rawConfig)
        );
    }

    /** Discovered field names per dataset definition; resolution runs on every query, sampling shouldn't. */
    private static final Map<String, CachedFields> FIELD_CACHE = new ConcurrentHashMap<>();
    static final long FIELD_CACHE_MILLIS = TimeUnit.MINUTES.toMillis(10);

    /** Names kept as a sorted list: a copied Set would lose the order and reshuffle the schema. */
    private record CachedFields(List<String> names, long expiresAt) {}

    private static List<String> discoveredFields(
        String location,
        Map<String, Object> rawConfig,
        BucketStore store,
        List<BucketDiscovery.BucketRef> buckets
    ) throws java.io.IOException {
        // the key covers everything that changes which buckets are sampled (not credentials)
        Map<String, Object> cfg = SplunkConfig.effective(rawConfig);
        String key = location + "|" + cfg.get("index") + "|" + cfg.get("index_pattern") + "|" + cfg.get("earliest") + "|" + cfg.get("latest");
        long now = System.currentTimeMillis();
        CachedFields cached = FIELD_CACHE.get(key);
        if (cached != null && cached.expiresAt() > now) {
            return cached.names();
        }
        List<String> names = List.copyOf(FieldDiscovery.sample(store, buckets));   // TreeSet order
        FIELD_CACHE.put(key, new CachedFields(names, now + FIELD_CACHE_MILLIS));
        return names;
    }

    @Override
    public Connector open(Map<String, Object> rawConfig) {
        return new SplunkConnector(SplunkConfig.parse(null, rawConfig));
    }

    @Override
    public SplitProvider splitProvider() {
        return SplunkConnectorFactory::discoverSplits;
    }

    static SplitDiscoveryResult discoverSplits(SplitDiscoveryContext context) {
        SplunkConfig config = SplunkConfig.parse(null, context.config());
        try (BucketStore store = config.openStore()) {
            List<BucketDiscovery.BucketRef> buckets = BucketDiscovery.discover(store, config);
            FilterHints hints = FilterHints.from(context.filterHints(), config.timestampField().equals(SplunkConfig.TIME));
            List<ExternalSplit> splits = new ArrayList<>(buckets.size());
            for (BucketDiscovery.BucketRef b : buckets) {
                if (hints.keeps(b)) {
                    splits.add(new BucketSplit(b));
                }
            }
            if (hints.prunes()) {
                logger.debug("[{}] {} kept {} of {} buckets", config.location(), hints, splits.size(), buckets.size());
            }
            if (splits.isEmpty()) {
                // An empty list makes the framework fall back to one unsplit scan of everything;
                // hand out a single split with no journal objects instead, which reads nothing.
                splits.add(new BucketSplit(new BucketDiscovery.BucketRef("", "", "", List.of(), -1, -1, 0)));
            }
            return new SplitDiscoveryResult(splits, buckets.size());
        } catch (Exception e) {
            throw new IllegalStateException("Failed Splunk bucket split discovery for [" + config.location() + "]: " + e.getMessage(), e);
        }
    }
}
