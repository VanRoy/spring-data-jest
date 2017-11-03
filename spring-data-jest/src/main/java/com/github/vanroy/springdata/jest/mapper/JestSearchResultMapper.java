package com.github.vanroy.springdata.jest.mapper;

import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import io.searchbox.core.SearchResult;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.springframework.data.domain.Pageable;

import java.util.List;

/**
 * Jest specific search result mapper.
 *
 * @author Julien Roy
 */
public interface JestSearchResultMapper {

	<T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable);

	<T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable);
}
