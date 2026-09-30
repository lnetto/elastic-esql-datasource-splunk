/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Parsed location + settings for a Splunk bucket dataset.
 *
 * <pre>
 *   splunkfs:///abs/path/under/path.repo      frozen archive / bucket tree on a shared filesystem
 *   splunks3://&lt;s3-bucket&gt;/&lt;prefix&gt;           SmartStore remote volume (or frozen buckets in S3)
 * </pre>
 */
record SplunkConfig(
    String location,
    String scheme,
    String root,
    String index,
    long earliest,
    long latest,
    List<String> fields,
    boolean autoFields,
    Pattern indexPattern,
    String timestampField,
    String s3Endpoint,
    String s3Region,
    boolean s3PathStyle,
    String accessKey,
    String secretKey,
    String sessionToken,
    Duration connectTimeout,
    Duration requestTimeout
) {

    static final String FS_SCHEME = "splunkfs";
    static final String S3_SCHEME = "splunks3";
    static final String TYPE = "splunk";
    static final String AUTO_FIELDS = "auto";
    static final String NO_FIELDS = "none";

    /** Key under which the resolver nests the parent data source's settings. */
    static final String DATASOURCE_ENVELOPE_KEY = "_datasource";
    /** Resolved location, carried from resolveMetadata to the data nodes. */
    static final String LOCATION_KEY = "location";

    static final Set<String> S3_KEYS = Set.of("endpoint", "region", "path_style", "access_key", "secret_key", "session_token");
    static final Set<String> READ_KEYS = Set.of(
        "index",
        "index_pattern",
        "timestamp_field",
        "earliest",
        "latest",
        "fields",
        // Kibana's Data Federation UI always sends a file format; journals need none, so it's ignored
        "format",
        "connect_timeout_ms",
        "request_timeout_ms"
    );
    /** Every key the connector accepts from the data source, the dataset, or the resolved config. */
    static final Set<String> CONFIG_KEYS;
    static {
        Set<String> all = new LinkedHashSet<>(S3_KEYS);
        all.addAll(READ_KEYS);
        all.add(LOCATION_KEY);
        CONFIG_KEYS = Set.copyOf(all);
    }

    /** Internal names for the two built-in timestamps (timestamp_field also accepts their column names). */
    static final String TIME = "_time";
    static final String INDEX_TIME = "_indextime";

    static boolean handles(String location) {
        return location != null && (location.startsWith(FS_SCHEME + "://") || location.startsWith(S3_SCHEME + "://"));
    }

    /**
     * Effective config: the _datasource envelope flattened underneath dataset/query-level keys
     * (which win), with _-prefixed internal bookkeeping dropped.
     */
    static Map<String, Object> effective(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> out = new HashMap<>();
        if (config.get(DATASOURCE_ENVELOPE_KEY) instanceof Map<?, ?> ds) {
            for (Map.Entry<?, ?> e : ds.entrySet()) {
                out.put(String.valueOf(e.getKey()), e.getValue());
            }
        }
        for (Map.Entry<String, Object> e : config.entrySet()) {
            if (e.getKey().startsWith("_") == false) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    static SplunkConfig parse(String location, Map<String, Object> rawConfig) {
        Map<String, Object> config = effective(rawConfig);
        if (location == null) {
            location = str(config, LOCATION_KEY, null);
        }
        if (handles(location) == false) {
            throw new IllegalArgumentException(
                "Splunk bucket location must be splunkfs:///path or splunks3://bucket/prefix, got [" + location + "]"
            );
        }
        URI uri = URI.create(location);
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        String root;
        if (scheme.equals(FS_SCHEME)) {
            if (uri.getHost() != null && uri.getHost().isEmpty() == false) {
                throw new IllegalArgumentException("splunkfs locations take an absolute path: splunkfs:///path, got [" + location + "]");
            }
            root = uri.getPath();
            if (root == null || root.startsWith("/") == false) {
                throw new IllegalArgumentException("splunkfs location needs an absolute path, got [" + location + "]");
            }
        } else {
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new IllegalArgumentException("splunks3 location needs an S3 bucket: splunks3://bucket/prefix, got [" + location + "]");
            }
            String path = uri.getPath() == null ? "" : uri.getPath();
            root = uri.getHost() + path;   // "<s3bucket>/<prefix>"
        }

        // fields: "auto" (default) discovers indexed fields from a sample; "none" exposes none;
        // a list names them explicitly; "auto,extra" does both.
        List<String> fields = new ArrayList<>();
        boolean autoFields = false;
        String fieldList = str(config, "fields", AUTO_FIELDS);
        for (String f : fieldList.split(",")) {
            String name = f.trim();
            if (name.isEmpty() || name.equalsIgnoreCase(NO_FIELDS)) {
                continue;
            }
            if (name.equalsIgnoreCase(AUTO_FIELDS)) {
                autoFields = true;
                continue;
            }
            if (name.startsWith(SplunkSchema.FIELDS_PREFIX)) {
                name = name.substring(SplunkSchema.FIELDS_PREFIX.length());   // accept column or Splunk name
            }
            if (fields.contains(name) == false) {
                fields.add(name);
            }
        }

        String endpoint = str(config, "endpoint", null);
        String region = str(config, "region", "us-east-1");
        boolean pathStyle = bool(config, "path_style", endpoint != null);
        if (endpoint == null) {
            endpoint = "https://s3." + region + ".amazonaws.com";
        }
        return new SplunkConfig(
            location,
            scheme,
            root,
            str(config, "index", null),
            epochSeconds(config.get("earliest")),
            epochSeconds(config.get("latest")),
            List.copyOf(fields),
            autoFields,
            indexPattern(str(config, "index_pattern", null)),
            timestampField(str(config, "timestamp_field", TIME)),
            endpoint,
            region,
            pathStyle,
            str(config, "access_key", null),
            str(config, "secret_key", null),
            str(config, "session_token", null),
            Duration.ofMillis(lng(config, "connect_timeout_ms", 5_000L)),
            Duration.ofMillis(lng(config, "request_timeout_ms", 300_000L))
        );
    }

    BucketStore openStore() {
        if (scheme.equals(FS_SCHEME)) {
            return new FsBucketStore(Path.of(root));
        }
        int slash = root.indexOf('/');
        String bucket = slash < 0 ? root : root.substring(0, slash);
        String prefix = slash < 0 ? "" : root.substring(slash + 1);
        return new S3BucketStore(
            bucket,
            prefix,
            URI.create(s3Endpoint),
            s3PathStyle,
            s3Region,
            accessKey,
            secretKey,
            sessionToken,
            connectTimeout,
            requestTimeout
        );
    }

    /** Name of the root directory/prefix, used as the index fallback for flat layouts. */
    String rootName() {
        String r = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
        return BucketDiscovery.lastSegment(r);
    }

    /** The config handed from resolveMetadata to split discovery and the data nodes. Secrets pass through untouched. */
    static Map<String, Object> resolved(String location, Map<String, Object> rawConfig) {
        Map<String, Object> config = effective(rawConfig);
        Map<String, Object> out = new HashMap<>();
        for (String key : CONFIG_KEYS) {
            Object v = config.get(key);
            if (v != null) {
                out.put(key, v);
            }
        }
        out.put(LOCATION_KEY, location);
        return out;
    }

    // Objects.toString: values may be SecureString or numbers; a (String) cast would CCE.
    private static String str(Map<String, Object> config, String key, String fallback) {
        String s = Objects.toString(config.get(key), null);
        return s == null || s.isEmpty() ? fallback : s;
    }

    private static boolean bool(Map<String, Object> config, String key, boolean fallback) {
        Object v = config.get(key);
        return v == null ? fallback : Boolean.TRUE.equals(v) || "true".equalsIgnoreCase(Objects.toString(v));
    }

    private static long lng(Map<String, Object> config, String key, long fallback) {
        Object v = config.get(key);
        if (v == null) {
            return fallback;
        }
        return v instanceof Number n ? n.longValue() : Long.parseLong(Objects.toString(v).trim());
    }

    /**
     * Normalises timestamp_field: {@code _time}/{@code splunk.time} (event time, default),
     * {@code _indextime}/{@code splunk.index_time}, or an indexed field by Splunk name or column name.
     */
    private static String timestampField(String name) {
        return switch (name) {
            case TIME, SplunkSchema.TIMESTAMP, SplunkSchema.TIME -> TIME;
            case INDEX_TIME, SplunkSchema.INDEX_TIME -> INDEX_TIME;
            default -> name.startsWith(SplunkSchema.FIELDS_PREFIX) ? name.substring(SplunkSchema.FIELDS_PREFIX.length()) : name;
        };
    }

    private static Pattern indexPattern(String regex) {
        if (regex == null) {
            return null;
        }
        try {
            Pattern p = Pattern.compile(regex);
            if (p.matcher("").groupCount() < 1) {
                throw new IllegalArgumentException("index_pattern needs a capture group for the index name, got [" + regex + "]");
            }
            return p;
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("index_pattern is not a valid regex: " + e.getDescription());
        }
    }

    /** Epoch seconds from a number, a numeric string, or an ISO-8601 instant. */
    static long epochSeconds(Object v) {
        if (v == null) {
            return BucketDiscovery.UNKNOWN;
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        String s = Objects.toString(v).trim();
        if (s.isEmpty()) {
            return BucketDiscovery.UNKNOWN;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            try {
                return Instant.parse(s).getEpochSecond();
            } catch (DateTimeParseException e2) {
                throw new IllegalArgumentException("expected epoch seconds or an ISO-8601 instant, got [" + s + "]");
            }
        }
    }
}
