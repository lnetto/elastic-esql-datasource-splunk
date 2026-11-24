/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

/**
 * {@link StorageObject} with the parts of the SPI that differ between Elasticsearch releases
 * (before 9.5.7, which has no storageIdentity()). Build.gradle picks this source set or es-9.5.7 by esVersion.
 */
interface CompatStorageObject extends StorageObject {}
