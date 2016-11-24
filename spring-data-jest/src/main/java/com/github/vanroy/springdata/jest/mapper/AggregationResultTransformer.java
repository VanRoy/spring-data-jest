package com.github.vanroy.springdata.jest.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.searchbox.core.search.aggregation.ExtendedStatsAggregation;
import io.searchbox.core.search.aggregation.HistogramAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.RangeAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsBuilder;
import org.springframework.data.elasticsearch.core.facet.FacetResult;
import org.springframework.data.elasticsearch.core.facet.result.HistogramResult;
import org.springframework.data.elasticsearch.core.facet.result.IntervalUnit;
import org.springframework.data.elasticsearch.core.facet.result.Range;
import org.springframework.data.elasticsearch.core.facet.result.RangeResult;
import org.springframework.data.elasticsearch.core.facet.result.StatisticalResult;
import org.springframework.data.elasticsearch.core.facet.result.Term;
import org.springframework.data.elasticsearch.core.facet.result.TermResult;

/**
 * Jest specific transformation from Jest Aggregation to SpringData facet.
 *
 * @author Julien Roy
 */
public class AggregationResultTransformer {

    public static List<FacetResult> parseAggregations(List<AbstractAggregationBuilder> queryAggregations, MetricAggregation aggregationResults) {

        if (Objects.isNull(queryAggregations)) {
            return Collections.emptyList();
        }

        return queryAggregations.stream().map(queryAggregation -> {

                String name = queryAggregation.getName();

                if (queryAggregation instanceof TermsBuilder) {
                    return parseTerms(aggregationResults.getTermsAggregation(name));
                } else if (queryAggregation instanceof ExtendedStatsBuilder) {
                    return parseExtendedStats(aggregationResults.getExtendedStatsAggregation(name));
                } else if (queryAggregation instanceof HistogramBuilder) {
                    return parseHistogram(aggregationResults.getHistogramAggregation(name));
                } else if (queryAggregation instanceof RangeBuilder) {
                    return parseRange(aggregationResults.getRangeAggregation(name));
                }

                return null;
            }
        )
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    }

    private static FacetResult parseTerms(TermsAggregation aggregation) {
        List<Term> entries = aggregation.getBuckets().stream()
                .map(entry -> new Term(entry.getKey(), entry.getCount().intValue()))
                .collect(Collectors.toList());
        return !entries.isEmpty() ? new TermResult(aggregation.getName(), entries, 0, 0, 0) : null;
    }

    private static FacetResult parseExtendedStats(ExtendedStatsAggregation stats) {
        return new StatisticalResult(stats.getName(), stats.getCount(), stats.getMax(), stats.getMin(), stats.getAvg(), stats.getStdDeviation(), stats.getSumOfSquares(), stats.getSum(), stats.getVariance());
    }

    private static FacetResult parseRange(RangeAggregation facet) {
        List<Range> entries = new ArrayList<>();
        for (io.searchbox.core.search.aggregation.Range entry : facet.getBuckets()) {
            entries.add(new Range(entry.getFrom() == Double.NEGATIVE_INFINITY ? null : entry.getFrom(), entry.getTo() == Double.POSITIVE_INFINITY ? null : entry.getTo(), entry.getCount(), 0, 0, 0, 0));
        }
        return new RangeResult(facet.getName(), entries);
    }

    private static FacetResult parseHistogram(HistogramAggregation facet) {
        List<IntervalUnit> entries = new ArrayList<>();
        for (HistogramAggregation.Histogram entry : facet.getBuckets()) {
            entries.add(new IntervalUnit(entry.getKey(), entry.getCount(), 0, 0, 0, 0, 0));
        }
        return new HistogramResult(facet.getName(), entries);
    }
}
