package com.github.vanroy.springdata.jest.aggregation;

import java.util.List;
import java.util.Map;

import com.github.vanroy.springdata.jest.facet.FacetedPage;
import io.searchbox.core.search.aggregation.Aggregation;

/**
 * @author Petar Tahchiev
 */
public interface AggregatedPage<T> extends FacetedPage<T> {

	boolean hasAggregations();

	List<? extends Aggregation> getAggregations(Map<String, Class> nameToTypeMap);

	<A extends Aggregation> A getAggregation(String aggName, Class<A> aggType);
}
