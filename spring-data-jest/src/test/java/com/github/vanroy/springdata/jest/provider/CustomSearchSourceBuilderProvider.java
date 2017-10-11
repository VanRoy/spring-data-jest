package com.github.vanroy.springdata.jest.provider;

import org.elasticsearch.search.builder.SearchSourceBuilder;

public class CustomSearchSourceBuilderProvider<T extends SearchSourceBuilder> extends DefaultSearchSourceBuilderProvider<T> {

    @Override
    public T get() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.trackScores(true);
        return (T)searchSourceBuilder;
    }
}
