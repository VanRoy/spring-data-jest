package com.github.vanroy.springdata.jest.aggregation.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import io.searchbox.core.search.aggregation.Aggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

/**
 * @author Petar Tahchiev
 * @author Artur Konczak
 * @author Mohsin Husen
 */
public class AggregatedPageImpl<T> extends PageImpl<T> implements AggregatedPage<T> {

	private final MetricAggregation aggregations;
	private String scrollId;

	public AggregatedPageImpl(List<T> content) {
		super(content);
		this.aggregations = null;
	}

	public AggregatedPageImpl(List<T> content, String scrollId) {
		super(content);
		this.scrollId = scrollId;
		this.aggregations = null;
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total) {
		super(content, pageable, total);
		this.aggregations = null;
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total, String scrollId) {
		super(content, pageable, total);
		this.scrollId = scrollId;
		this.aggregations = null;
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total, MetricAggregation aggregations) {
		super(content, pageable, total);
		this.aggregations = aggregations;
	}

	public AggregatedPageImpl(List<T> content, Pageable pageable, long total, MetricAggregation aggregations, String scrollId) {
		super(content, pageable, total);
		this.aggregations = aggregations;
		this.scrollId = scrollId;
	}

	@Override
	public boolean hasAggregations() {
		return !Objects.isNull(aggregations);
	}

	@Override
	public List<? extends Aggregation> getAggregations(Map<String, Class> nameToTypeMap) {
		return hasAggregations() ? aggregations.getAggregations(nameToTypeMap) : Collections.emptyList();
	}

	@Override
	public <A extends Aggregation> A getAggregation(String aggName, Class<A> aggType) {
		return hasAggregations() ? aggregations.getAggregation(aggName, aggType) : null;
	}

	@Override
	public String getScrollId() {
		return scrollId;
	}
}
