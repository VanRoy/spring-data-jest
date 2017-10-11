package com.github.vanroy.springdata.jest.provider;

import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class DefaultSearchSourceBuilderProvider<T extends SearchSourceBuilder> implements Provider<T> {
    @Override
    public T get() {
        return (T)new SearchSourceBuilder();
    }
}
