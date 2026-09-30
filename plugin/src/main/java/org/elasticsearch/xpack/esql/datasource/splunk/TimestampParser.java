/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

/**
 * Parses an indexed field's text into epoch millis for {@code @timestamp} ({@code timestamp_field}).
 * Accepts ISO-8601 date-times with {@code T} or a space, any fraction, and an optional offset in
 * {@code Z}, {@code +00:00} or Splunk's {@code +0000} form (no offset = UTC); or a plain number,
 * read as epoch seconds (up to 11 digits, fraction allowed) or epoch millis. Anything else is null.
 */
final class TimestampParser {

    private static final DateTimeFormatter ISO = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral('T')
        .optionalEnd()
        .optionalStart()
        .appendLiteral(' ')
        .optionalEnd()
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .optionalEnd()
        .optionalStart()
        .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HH:MM", "Z")
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HHMM", "Z")
        .optionalEnd()
        .toFormatter(java.util.Locale.ROOT);

    private static final java.util.regex.Pattern EPOCH = java.util.regex.Pattern.compile("\\d+(\\.\\d+)?");

    private TimestampParser() {}

    /** Epoch millis, or null when the text isn't a recognisable timestamp. */
    static Long parseMillis(byte[] value) {
        if (value == null || value.length == 0) {
            return null;
        }
        String s = new String(value, StandardCharsets.UTF_8).trim();
        if (s.isEmpty()) {
            return null;
        }
        if (EPOCH.matcher(s).matches()) {
            return epoch(s);
        }
        try {
            TemporalAccessor t = ISO.parse(s);
            LocalDateTime local = LocalDateTime.of(
                t.get(ChronoField.YEAR),
                t.get(ChronoField.MONTH_OF_YEAR),
                t.get(ChronoField.DAY_OF_MONTH),
                t.get(ChronoField.HOUR_OF_DAY),
                t.get(ChronoField.MINUTE_OF_HOUR),
                t.isSupported(ChronoField.SECOND_OF_MINUTE) ? t.get(ChronoField.SECOND_OF_MINUTE) : 0,
                t.isSupported(ChronoField.NANO_OF_SECOND) ? t.get(ChronoField.NANO_OF_SECOND) : 0
            );
            ZoneOffset offset = t.isSupported(ChronoField.OFFSET_SECONDS)
                ? ZoneOffset.ofTotalSeconds(t.get(ChronoField.OFFSET_SECONDS))
                : ZoneOffset.UTC;
            return local.toInstant(offset).toEpochMilli();
        } catch (java.time.DateTimeException e) {                     // includes DateTimeParseException
            return null;
        }
    }

    private static Long epoch(String s) {
        try {
            int dot = s.indexOf('.');
            String whole = dot < 0 ? s : s.substring(0, dot);
            if (whole.length() <= 11) {                                  // seconds (until year 5138)
                return Math.round(Double.parseDouble(s) * 1000d);
            }
            return dot < 0 ? Long.parseLong(s) : (long) Double.parseDouble(s);   // millis
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
