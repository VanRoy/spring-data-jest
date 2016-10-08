package com.github.vanroy.springdata.jest.mapper;

import java.util.List;

import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import io.searchbox.core.SearchResult;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.springframework.data.domain.Pageable;

/**
 * Jest specific search result mapper.
 *
 * @author Julien Roy
 */
public interface JestSearchResultMapper {

	<T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable);

	<T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable, List<AbstractAggregationBuilder> aggregations);
}
