package com.github.vanroy.springdata.jest.mapper;

import io.searchbox.core.SearchResult;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.query.SearchQuery;

/**
 * Jest specific search result mapper.
 *
 * @author Julien Roy
 */
public interface JestSearchResultMapper {

	<T> Page<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable);
}
