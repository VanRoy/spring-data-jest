package com.github.vanroy.springdata.jest.mapper;

import io.searchbox.action.Action;
import io.searchbox.client.JestResult;

/**
 * Error mapper interface used to check JestResult and throw exception if needed.
 *
 * @author Julien
 */
public interface ErrorMapper {

	void mapError(Action action, JestResult result, boolean acceptNotFound);
}
