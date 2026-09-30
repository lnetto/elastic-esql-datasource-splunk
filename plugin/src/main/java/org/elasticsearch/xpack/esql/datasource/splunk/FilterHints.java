/*
 * Splunk bucket connector for ES|QL Data Federation.
 */

package org.elasticsearch.xpack.esql.datasource.splunk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Bucket pruning from the query's WHERE clause. ES|QL hands split discovery the filters sitting
 * above the source; they are still evaluated on every row afterwards, so this only has to be
 * conservative: understand {@code splunk.time} bounds (and {@code @timestamp} while it is the event time) and {@code splunk.index ==}/{@code IN}, ignore the rest.
 * Inclusive/exclusive bounds are both treated as inclusive (never prunes a bucket it shouldn't).
 */
final class FilterHints {

    long earliestMs = Long.MIN_VALUE;
    long latestMs = Long.MAX_VALUE;
    /** Allowed index names, or null for "any". */
    Set<String> indexes;

    /** Whether {@code @timestamp} currently is the event time (timestamp_field default); if not, only splunk.time prunes. */
    private boolean timestampIsEventTime = true;

    static FilterHints from(List<Expression> filters) {
        return from(filters, true);
    }

    static FilterHints from(List<Expression> filters, boolean timestampIsEventTime) {
        FilterHints h = new FilterHints();
        h.timestampIsEventTime = timestampIsEventTime;
        if (filters != null) {
            for (Expression f : filters) {
                h.visit(f);
            }
        }
        return h;
    }

    private void visit(Expression e) {
        if (e instanceof And and) {
            visit(and.left());
            visit(and.right());
        } else if (e instanceof Range r && isTime(r.value())) {
            lowerTime(r.lower());
            upperTime(r.upper());
        } else if (e instanceof In in && isField(in.value(), SplunkSchema.INDEX)) {
            Set<String> names = new HashSet<>();
            for (Expression v : in.list()) {
                String s = keyword(v);
                if (s == null) {
                    return;                        // non-literal member: can't prune on it
                }
                names.add(s);
            }
            restrictIndexes(names);
        } else if (e instanceof EsqlBinaryComparison c) {
            boolean fieldLeft = isTime(c.left()) || isField(c.left(), SplunkSchema.INDEX);
            Expression field = fieldLeft ? c.left() : c.right();
            Expression value = fieldLeft ? c.right() : c.left();
            // normalise "literal OP field" to "field OP' literal"
            boolean greater = c instanceof GreaterThan || c instanceof GreaterThanOrEqual;
            boolean less = c instanceof LessThan || c instanceof LessThanOrEqual;
            if (fieldLeft == false) {
                boolean swap = greater;
                greater = less;
                less = swap;
            }
            if (isTime(field)) {
                if (greater || c instanceof Equals) {
                    lowerTime(value);
                }
                if (less || c instanceof Equals) {
                    upperTime(value);
                }
            } else if (isField(field, SplunkSchema.INDEX) && c instanceof Equals) {
                String s = keyword(value);
                if (s != null) {
                    restrictIndexes(Set.of(s));
                }
            }
        }
    }

    private void lowerTime(Expression v) {
        Long ms = millis(v);
        if (ms != null) {
            earliestMs = Math.max(earliestMs, ms);
        }
    }

    private void upperTime(Expression v) {
        Long ms = millis(v);
        if (ms != null) {
            latestMs = Math.min(latestMs, ms);
        }
    }

    private void restrictIndexes(Set<String> names) {
        if (indexes == null) {
            indexes = new HashSet<>(names);
        } else {
            indexes.retainAll(names);
        }
    }

    boolean keeps(BucketDiscovery.BucketRef b) {
        if (indexes != null && indexes.contains(b.index()) == false) {
            return false;
        }
        // bucket ranges are whole seconds: [oldest, newest + 1s)
        if (b.newest() != BucketDiscovery.UNKNOWN && earliestMs != Long.MIN_VALUE && (b.newest() + 1) * 1000L <= earliestMs) {
            return false;
        }
        return b.oldest() == BucketDiscovery.UNKNOWN || latestMs == Long.MAX_VALUE || b.oldest() * 1000L <= latestMs;
    }

    boolean prunes() {
        return indexes != null || earliestMs != Long.MIN_VALUE || latestMs != Long.MAX_VALUE;
    }

    /** Bucket ranges are event-time ranges: only a column holding the event time may prune on them. */
    private boolean isTime(Expression e) {
        return isField(e, SplunkSchema.TIME) || (timestampIsEventTime && isField(e, SplunkSchema.TIMESTAMP));
    }

    private static boolean isField(Expression e, String name) {
        return e instanceof Attribute a && a.name().equals(name);
    }

    private static Long millis(Expression e) {
        return e instanceof Literal l && l.value() instanceof Long v ? v : null;
    }

    private static String keyword(Expression e) {
        if (e instanceof Literal l) {
            if (l.value() instanceof BytesRef b) {
                return b.utf8ToString();
            }
            if (l.value() instanceof String s) {
                return s;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "FilterHints[time=" + earliestMs + ".." + latestMs + ", indexes=" + indexes + "]";
    }
}
