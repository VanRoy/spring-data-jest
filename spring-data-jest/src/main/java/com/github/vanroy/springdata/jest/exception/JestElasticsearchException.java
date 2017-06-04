package com.github.vanroy.springdata.jest.exception;

import io.searchbox.client.JestResult;

/**
 * Exception throw when error occur on Elasticsearch request.
 *
 * @author Julien Roy
 */
public class JestElasticsearchException extends RuntimeException {

    private final JestResult result;

    public JestElasticsearchException(String message, JestResult result) {
        super(message);
        this.result = result;
    }

    public JestResult getResult() {
        return result;
    }
}
