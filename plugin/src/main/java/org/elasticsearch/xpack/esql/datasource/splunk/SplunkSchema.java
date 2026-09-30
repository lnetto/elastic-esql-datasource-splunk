/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * The event schema, shaped like the documents the {@code s2s-http} plugin indexes from live Splunk
 * forwarders (ECS plus {@code splunk.*}), so the same queries run on forwarded and bucket data.
 *
 * <pre>
 *   {@literal @}timestamp          datetime   event time; timestamp_field can pick splunk.index_time or an indexed field
 *   message             keyword    the raw event (_raw), byte for byte
 *   host.name           keyword    Splunk host
 *   log.file.path       keyword    Splunk source, when it is a file path (else null)
 *   splunk.index        keyword
 *   splunk.host         keyword
 *   splunk.source       keyword
 *   splunk.sourcetype   keyword
 *   splunk.time         datetime   Splunk's _time, always (even when timestamp_field moves @timestamp)
 *   splunk.index_time   datetime   when Splunk indexed the event (_indextime)       [bucket-only]
 *   splunk.bucket       keyword    bucket the event was read from                  [bucket-only]
 *   splunk.fields.NAME  keyword    each indexed field (multi-valued if repeated)
 * </pre>
 */
final class SplunkSchema {

    static final String TIMESTAMP = "@timestamp";
    static final String MESSAGE = "message";
    static final String HOST_NAME = "host.name";
    static final String LOG_FILE_PATH = "log.file.path";
    static final String INDEX = "splunk.index";
    static final String HOST = "splunk.host";
    static final String SOURCE = "splunk.source";
    static final String SOURCETYPE = "splunk.sourcetype";
    static final String TIME = "splunk.time";
    static final String INDEX_TIME = "splunk.index_time";
    static final String BUCKET = "splunk.bucket";
    static final String FIELDS_PREFIX = "splunk.fields.";

    private SplunkSchema() {}

    static List<Attribute> attributes(List<String> indexedFields) {
        return attributes(indexedFields, SplunkConfig.TIME);
    }

    /** @param timestampField what fills {@code @timestamp}; an indexed field makes it nullable (missing/unparseable). */
    static List<Attribute> attributes(List<String> indexedFields, String timestampField) {
        boolean builtIn = timestampField.equals(SplunkConfig.TIME) || timestampField.equals(SplunkConfig.INDEX_TIME);
        List<Attribute> out = new ArrayList<>(10 + indexedFields.size());
        out.add(attr(TIMESTAMP, DataType.DATETIME, builtIn ? Nullability.FALSE : Nullability.TRUE));
        out.add(attr(MESSAGE, DataType.KEYWORD, Nullability.FALSE));
        out.add(attr(HOST_NAME, DataType.KEYWORD, Nullability.FALSE));
        out.add(attr(LOG_FILE_PATH, DataType.KEYWORD, Nullability.TRUE));
        out.add(attr(INDEX, DataType.KEYWORD, Nullability.FALSE));
        out.add(attr(HOST, DataType.KEYWORD, Nullability.FALSE));
        out.add(attr(SOURCE, DataType.KEYWORD, Nullability.FALSE));
        out.add(attr(SOURCETYPE, DataType.KEYWORD, Nullability.FALSE));
        out.add(attr(TIME, DataType.DATETIME, Nullability.FALSE));
        out.add(attr(INDEX_TIME, DataType.DATETIME, Nullability.FALSE));
        out.add(attr(BUCKET, DataType.KEYWORD, Nullability.FALSE));
        for (String f : indexedFields) {
            out.add(attr(FIELDS_PREFIX + f, DataType.KEYWORD, Nullability.TRUE));
        }
        return out;
    }

    /** Same rule as s2s-http: a source is a file path when it's absolute (Unix or Windows drive). */
    static boolean isFilePath(byte[] source) {
        if (source.length == 0) {
            return false;
        }
        if (source[0] == '/') {
            return true;
        }
        return source.length >= 3 && Character.isLetter(source[0]) && source[1] == ':' && source[2] == '\\';
    }

    private static Attribute attr(String name, DataType type, Nullability nullability) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, nullability, null, false);
    }
}
