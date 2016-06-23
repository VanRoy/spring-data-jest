package com.github.vanroy.springdata.jest.mapper;

import java.util.LinkedList;

import com.github.vanroy.springdata.jest.internal.MultiDocumentResult;

/**
 * Jest specific multi get result mapper.
 *
 * @author Julien Roy
 */
public interface JestMultiGetResultMapper {

	<T> LinkedList<T> mapResults(MultiDocumentResult response, Class<T> clazz);
}
