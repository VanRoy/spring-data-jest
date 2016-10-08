package com.github.vanroy.springdata.jest.facet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

/**
 * Container for query result and facet results
 *
 * @author Rizwan Idrees
 * @author Mohsin Husen
 * @author Artur Konczak
 * @author Jonathan Yan
 */
@Deprecated
public abstract class FacetedPageImpl<T> extends PageImpl<T> implements FacetedPage<T>, AggregatedPage<T> {


    private List<FacetResult> facets;
    private Map<String, FacetResult> mapOfFacets = new HashMap<>();

    public FacetedPageImpl(List<T> content) {
        super(content);
    }

    public FacetedPageImpl(List<T> content, Pageable pageable, long total) {
        super(content, pageable, total);
    }

    public FacetedPageImpl(List<T> content, Pageable pageable, long total, List<FacetResult> facets) {
        super(content, pageable, total);
        this.facets = facets;
        if (Objects.nonNull(facets)) {
            this.mapOfFacets = facets.stream().collect(Collectors.toMap(FacetResult::getName, Function.identity()));
        }
    }

    @Override
    public boolean hasFacets() {
        return facets != null && !facets.isEmpty();
    }

    @Override
    public List<FacetResult> getFacets() {
        return facets;
    }

    @Override
    public FacetResult getFacet(String name) {
        return mapOfFacets.get(name);
    }

    private void addFacet(FacetResult facetResult) {
        facets.add(facetResult);
        mapOfFacets.put(facetResult.getName(), facetResult);
    }
}