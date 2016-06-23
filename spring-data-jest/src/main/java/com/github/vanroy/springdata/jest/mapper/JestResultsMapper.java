package com.github.vanroy.springdata.jest.mapper;

import org.springframework.data.elasticsearch.core.EntityMapper;

/**
 * Jest specific result mapper.
 *
 * @author Julien Roy
 */
public interface JestResultsMapper extends JestSearchResultMapper, JestGetResultMapper, JestMultiGetResultMapper, JestScrollResultMapper {

	EntityMapper getEntityMapper();
}
