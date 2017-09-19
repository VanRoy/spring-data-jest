package com.github.vanroy.springdata.jest.aggregation;

import io.searchbox.core.search.aggregation.Aggregation;
import org.springframework.data.domain.Page;
import org.springframework.data.elasticsearch.core.ScrolledPage;

import java.util.List;
import java.util.Map;

/**
 * @author Petar Tahchiev
 */
public interface AggregatedPage<T> extends Page<T>, ScrolledPage<T> {

	boolean hasAggregations();

	List<? extends Aggregation> getAggregations(Map<String, Class> nameToTypeMap);

	<A extends Aggregation> A getAggregation(String aggName, Class<A> aggType);
}
