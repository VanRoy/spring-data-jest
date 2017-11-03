package com.github.vanroy.springdata.jest.mapper;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.github.vanroy.springdata.jest.internal.SearchScrollResult;

/**
 * Jest specific scroll result mapper.
 *
 * @author Julien Roy
 */
public interface JestScrollResultMapper {

	<T> Page<T> mapResults(SearchScrollResult response, Class<T> clazz);
}
