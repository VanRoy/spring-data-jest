package com.github.vanroy.springdata.jest.aggregation;

import java.util.List;
import java.util.Map;

import io.searchbox.core.search.aggregation.Aggregation;
import org.springframework.data.elasticsearch.core.FacetedPage;

/**
 * @author Petar Tahchiev
 */
public interface AggregatedPage<T> extends FacetedPage<T> {

	boolean hasAggregations();

	List<? extends Aggregation> getAggregations(Map<String, Class> nameToTypeMap);

	<A extends Aggregation> A getAggregation(String aggName, Class<A> aggType);
}
