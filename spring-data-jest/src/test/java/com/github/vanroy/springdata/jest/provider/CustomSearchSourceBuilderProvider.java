package com.github.vanroy.springdata.jest.provider;

import java.util.function.Supplier;

import org.elasticsearch.search.builder.SearchSourceBuilder;

public class CustomSearchSourceBuilderProvider implements Supplier<SearchSourceBuilder> {

    @Override
    public SearchSourceBuilder get() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.trackScores(true);
        return searchSourceBuilder;
    }
}
