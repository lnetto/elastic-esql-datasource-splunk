/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Buckets in an S3-compatible object store — a SmartStore remote volume, or frozen buckets
 * archived to S3. Deliberately tiny: ListObjectsV2 + GetObject, signed with SigV4 using
 * static credentials (or anonymous), over the JDK HttpClient pinned to HTTP/1.1.
 * Works against AWS S3 and MinIO (set {@code path_style}).
 */
final class S3BucketStore implements BucketStore {

    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.ROOT);
    private static final String EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private final String bucket;
    private final String prefix;          // "" or ends with '/'
    private final URI endpoint;
    private final boolean pathStyle;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final Duration requestTimeout;
    private final HttpClient client;

    S3BucketStore(
        String bucket,
        String prefix,
        URI endpoint,
        boolean pathStyle,
        String region,
        String accessKey,
        String secretKey,
        String sessionToken,
        Duration connectTimeout,
        Duration requestTimeout
    ) {
        this.bucket = bucket;
        this.prefix = prefix.isEmpty() || prefix.endsWith("/") ? prefix : prefix + "/";
        this.endpoint = endpoint;
        this.pathStyle = pathStyle;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
        this.requestTimeout = requestTimeout;
        // HTTP/1.1: the JDK client otherwise attempts an h2c upgrade on plaintext connections,
        // which MinIO and some proxies mishandle.
        this.client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(connectTimeout)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    }

    @Override
    public void list(Consumer<Entry> sink) throws IOException {
        String token = null;
        do {
            Map<String, String> query = new TreeMap<>();
            query.put("list-type", "2");
            query.put("max-keys", "1000");
            if (prefix.isEmpty() == false) {
                query.put("prefix", prefix);
            }
            if (token != null) {
                query.put("continuation-token", token);
            }
            try (InputStream body = send("", query)) {
                token = parseListPage(body, sink);
            }
        } while (token != null);
    }

    /** Emits the page's entries; returns the continuation token or null on the last page. */
    private String parseListPage(InputStream body, Consumer<Entry> sink) throws IOException {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        String next = null;
        boolean truncated = false;
        try {
            XMLStreamReader r = factory.createXMLStreamReader(body);
            String key = null;
            long size = -1;
            while (r.hasNext()) {
                int ev = r.next();
                if (ev == XMLStreamConstants.START_ELEMENT) {
                    String name = r.getLocalName();
                    switch (name) {
                        case "Contents" -> {
                            key = null;
                            size = -1;
                        }
                        case "Key" -> key = r.getElementText();
                        case "Size" -> size = Long.parseLong(r.getElementText().trim());
                        case "NextContinuationToken" -> next = r.getElementText();
                        case "IsTruncated" -> truncated = Boolean.parseBoolean(r.getElementText().trim());
                        default -> {
                        }
                    }
                } else if (ev == XMLStreamConstants.END_ELEMENT) {
                    if (r.getLocalName().equals("Contents") && key != null && key.startsWith(prefix)) {
                        String rel = key.substring(prefix.length());
                        if (BucketStore.interesting(rel)) {
                            sink.accept(new Entry(rel, size));
                        }
                    }
                }
            }
            r.close();
        } catch (XMLStreamException | NumberFormatException e) {
            throw new IOException("unparseable ListObjectsV2 response from " + describe(), e);
        }
        return truncated ? next : null;
    }

    @Override
    public InputStream open(String key) throws IOException {
        return send(prefix + key, Map.of());
    }

    private InputStream send(String objectKey, Map<String, String> query) throws IOException {
        String canonicalPath = (pathStyle ? "/" + uriEncode(bucket, false) : "") + "/" + uriEncode(objectKey, false);
        String host = pathStyle ? endpoint.getHost() : bucket + "." + endpoint.getHost();
        if (endpoint.getPort() > 0) {
            host += ":" + endpoint.getPort();
        }
        StringBuilder qs = new StringBuilder();
        for (Map.Entry<String, String> e : new TreeMap<>(query).entrySet()) {
            if (qs.length() > 0) {
                qs.append('&');
            }
            qs.append(uriEncode(e.getKey(), true)).append('=').append(uriEncode(e.getValue(), true));
        }
        URI uri = URI.create(endpoint.getScheme() + "://" + host + canonicalPath + (qs.length() > 0 ? "?" + qs : ""));
        HttpRequest.Builder req = HttpRequest.newBuilder(uri).timeout(requestTimeout).GET();
        if (accessKey != null && accessKey.isEmpty() == false) {
            sign(req, host, canonicalPath, qs.toString());
        }
        HttpResponse<InputStream> response;
        try {
            response = client.send(req.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted calling " + describe(), e);
        }
        if (response.statusCode() != 200) {
            String error;
            try (InputStream err = response.body()) {
                error = new String(err.readNBytes(2048), StandardCharsets.UTF_8);
            }
            throw new IOException("S3 GET [" + (objectKey.isEmpty() ? bucket : objectKey) + "] returned HTTP " + response.statusCode() + ": " + error);
        }
        return response.body();
    }

    private void sign(HttpRequest.Builder req, String host, String canonicalPath, String canonicalQuery) throws IOException {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String amzDate = AMZ_DATE.format(now);
        String day = amzDate.substring(0, 8);
        TreeMap<String, String> headers = new TreeMap<>();
        headers.put("host", host);
        headers.put("x-amz-content-sha256", EMPTY_SHA256);
        headers.put("x-amz-date", amzDate);
        if (sessionToken != null && sessionToken.isEmpty() == false) {
            headers.put("x-amz-security-token", sessionToken);
        }
        StringBuilder canonicalHeaders = new StringBuilder();
        for (Map.Entry<String, String> h : headers.entrySet()) {
            canonicalHeaders.append(h.getKey()).append(':').append(h.getValue().trim()).append('\n');
        }
        String signedHeaders = String.join(";", headers.keySet());
        String canonicalRequest = "GET\n"
            + canonicalPath
            + "\n"
            + canonicalQuery
            + "\n"
            + canonicalHeaders
            + "\n"
            + signedHeaders
            + "\n"
            + EMPTY_SHA256;
        String scope = day + "/" + region + "/s3/aws4_request";
        String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex(canonicalRequest);
        try {
            byte[] k = hmac(("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8), day);
            k = hmac(k, region);
            k = hmac(k, "s3");
            k = hmac(k, "aws4_request");
            String signature = HexFormat.of().formatHex(hmac(k, stringToSign));
            req.header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + scope + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature
            );
        } catch (GeneralSecurityException e) {
            throw new IOException("SigV4 signing failed", e);
        }
        // "host" is set by the JDK client from the URI (restricted header); it matches what we signed.
        for (Map.Entry<String, String> h : headers.entrySet()) {
            if (h.getKey().equals("host") == false) {
                req.header(h.getKey(), h.getValue());
            }
        }
    }

    private static byte[] hmac(byte[] key, String data) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256Hex(String s) throws IOException {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
    }

    /** RFC 3986 encoding as SigV4 wants it: unreserved chars pass, '/' kept in paths. */
    static String uriEncode(String s, boolean encodeSlash) {
        StringBuilder out = new StringBuilder(s.length() + 16);
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~') {
                out.append((char) c);
            } else if (c == '/' && encodeSlash == false) {
                out.append('/');
            } else {
                out.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16))).append(Character.toUpperCase(Character.forDigit(c & 0xF, 16)));
            }
        }
        return out.toString();
    }

    @Override
    public String describe() {
        return "s3://" + bucket + "/" + prefix;
    }

    @Override
    public void close() {
        if (client instanceof AutoCloseable ac) {
            try {
                ac.close();
            } catch (Exception e) {
                // best effort
            }
        }
    }
}
