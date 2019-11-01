package com.github.vanroy.springdata.jest.mapper;

import com.github.vanroy.springdata.jest.internal.SearchScrollResult;
import org.springframework.data.elasticsearch.core.ScrolledPage;

/**
 * Jest specific scroll result mapper.
 *
 * @author Julien Roy
 */
public interface JestScrollResultMapper {

	<T> ScrolledPage<T> mapResults(SearchScrollResult response, Class<T> clazz);
}
