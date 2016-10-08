package com.github.vanroy.springdata.jest.aggregation.impl;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import com.github.vanroy.springdata.jest.facet.FacetResult;
import com.github.vanroy.springdata.jest.facet.FacetedPageImpl;
import io.searchbox.core.search.aggregation.Aggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import org.springframework.data.domain.Pageable;

/**
 * @author Petar Tahchiev
 * @author Artur Konczak
 * @author Mohsin Husen
 */
public class AggregatedPageImpl<T> extends FacetedPageImpl<T> implements AggregatedPage<T> {

	private final MetricAggregation aggregations;

	public AggregatedPageImpl(List<T> content) {
		super(content);
		this.aggregations = null;
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total) {
		super(content, pageable, total);
		this.aggregations = null;
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total, MetricAggregation aggregations, List<FacetResult> facets) {
		super(content, pageable, total, facets);
		this.aggregations = aggregations;
	}

	@Override
	public boolean hasAggregations() {
		return !Objects.isNull(aggregations);
	}

	@Override
	public List<? extends Aggregation> getAggregations(Map<String, Class> nameToTypeMap) {
		return aggregations.getAggregations(nameToTypeMap);
	}

	@Override
	public <A extends Aggregation> A getAggregation(String aggName, Class<A> aggType) {
		return aggregations.getAggregation(aggName, aggType);
	}

}
