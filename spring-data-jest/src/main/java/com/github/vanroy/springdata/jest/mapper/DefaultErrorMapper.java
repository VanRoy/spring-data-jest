package com.github.vanroy.springdata.jest.mapper;

import com.github.vanroy.springdata.jest.exception.JestElasticsearchException;
import com.google.gson.JsonPrimitive;
import io.searchbox.action.Action;
import io.searchbox.client.JestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation for ErrorMapper.
 *
 * @author Julien Roy
 */
public class DefaultErrorMapper implements ErrorMapper {

	private static final Logger logger = LoggerFactory.getLogger(DefaultErrorMapper.class);

	@Override
	public void mapError(Action action, JestResult result, boolean acceptNotFound) {

		if (!result.isSucceeded()) {

			String errorMessage = String.format("Cannot execute jest action , response code : %s , error : %s , message : %s", result.getResponseCode(), result.getErrorMessage(), getMessage(result));

			if (acceptNotFound && isSuccessfulResponse(result.getResponseCode())) {
				logger.debug(errorMessage);
			} else {
				logger.error(errorMessage);
				throw new JestElasticsearchException(errorMessage, result);
			}
		}
	}

	private static <T extends JestResult> String getMessage(T result) {
		if (result.getJsonObject() == null) {
			return null;
		}
		JsonPrimitive message = result.getJsonObject().getAsJsonPrimitive("message");
		if (message == null) {
			return null;
		}
		return message.getAsString();
	}

	private static boolean isSuccessfulResponse(int statusCode) {
		return statusCode < 300 || statusCode == 404;
	}
}
