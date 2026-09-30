/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Reads Splunk buckets. With a {@link BucketSplit} it decodes exactly that bucket (the parallel
 * path); with {@link Split#SINGLE} it discovers and decodes every bucket in order.
 *
 * <p>Pushed down: column projection (unrequested columns are never materialised, indexed fields
 * are not even decoded) and LIMIT (decoding stops). Time pruning is available via the dataset's
 * {@code earliest}/{@code latest} settings; ES|QL WHERE clauses are evaluated after the rows arrive.
 */
final class SplunkConnector implements Connector {

    private final SplunkConfig config;
    private final BucketStore store;

    SplunkConnector(SplunkConfig config) {
        this.config = config;
        this.store = config.openStore();
    }

    @Override
    public ResultCursor execute(QueryRequest request, Split split) {
        List<BucketDiscovery.BucketRef> buckets;
        try {
            buckets = BucketDiscovery.discover(store, config);
        } catch (IOException e) {
            throw new UncheckedIOException("failed listing Splunk buckets under " + store.describe(), e);
        }
        return cursor(request, buckets);
    }

    @Override
    public ResultCursor execute(QueryRequest request, ExternalSplit split) {
        if (split instanceof BucketSplit bs) {
            return cursor(request, List.of(bs.bucket()));
        }
        return execute(request, Split.SINGLE);
    }

    private ResultCursor cursor(QueryRequest request, List<BucketDiscovery.BucketRef> buckets) {
        int limit = request.rowLimit();
        return new BucketResultCursor(
            store,
            buckets,
            request.attributes(),
            request.blockFactory(),
            request.batchSize(),
            limit == FormatReader.NO_LIMIT || limit < 0 ? -1 : limit,
            config.earliest(),
            config.latest(),
            config.timestampField()
        );
    }

    @Override
    public void close() throws IOException {
        store.close();
    }

    @Override
    public String toString() {
        return "SplunkConnector[" + store.describe() + "]";
    }
}
