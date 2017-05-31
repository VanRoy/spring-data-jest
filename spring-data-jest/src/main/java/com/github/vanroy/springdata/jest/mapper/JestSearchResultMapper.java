package com.github.vanroy.springdata.jest.mapper;

import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import io.searchbox.core.SearchResult;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

import java.util.List;

/**
 * Jest specific search result mapper.
 *
 * @author Julien Roy
 */
public interface JestSearchResultMapper {

	<T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz);

	<T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations);
}
