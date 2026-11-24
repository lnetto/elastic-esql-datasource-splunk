/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

/**
 * {@link StorageObject} with the parts of the SPI that differ between Elasticsearch releases
 * (Elasticsearch 9.5.7 and later). Build.gradle picks this source set or es-pre-9.5.7 by esVersion.
 */
interface CompatStorageObject extends StorageObject {

    @Override
    default StorageIdentity storageIdentity() {
        // A bucket tree changes as Splunk freezes/uploads buckets, so never share cached state.
        return StorageIdentity.unique();
    }
}
