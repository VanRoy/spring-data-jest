/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.vanroy.springdata.jest.mapper;

import java.util.ArrayList;
import java.util.List;

import io.searchbox.core.SearchResult;
import io.searchbox.core.search.facet.*;
import org.springframework.data.elasticsearch.core.facet.FacetResult;
import org.springframework.data.elasticsearch.core.facet.result.*;

/**
 * @author Julien Roy
 */
public class JestDefaultFacetMapper {

	public static List<FacetResult> parse(SearchResult result) {

		List<FacetResult> facets = new ArrayList<FacetResult>();

		parseFacets(result.getFacets(TermsFacet.class), facets);
		parseFacets(result.getFacets(RangeFacet.class), facets);
		parseFacets(result.getFacets(StatisticalFacet.class), facets);
		parseFacets(result.getFacets(HistogramFacet.class), facets);

		return facets;
	}

	public static FacetResult parse(Facet facet) {

		if (facet instanceof TermsFacet) {
			return parseTerm((TermsFacet) facet);
		}

		if (facet instanceof RangeFacet) {
			return parseRange((RangeFacet) facet);
		}

		if (facet instanceof StatisticalFacet) {
			return parseStatistical((StatisticalFacet) facet);
		}

		if (facet instanceof HistogramFacet) {
			return parseHistogram((HistogramFacet) facet);
		}

		return null;
	}

	private static List<FacetResult> parseFacets(List<? extends Facet> facets, List<FacetResult> results) {
		if (facets != null) {
			for (Facet facet : facets) {
				FacetResult facetResult = parse(facet);
				if (facetResult != null) {
					results.add(facetResult);
				}
			}
		}

		return results;
	}

	private static FacetResult parseTerm(TermsFacet facet) {
		List<Term> entries = new ArrayList<Term>();
		for (TermsFacet.Term entry : facet.terms()) {
			entries.add(new Term(entry.getName(), entry.getCount()));
		}
		return new TermResult(facet.getName(), entries, facet.getTotal(), facet.getOther(), facet.getMissing());
	}

	private static FacetResult parseRange(RangeFacet facet) {
		List<Range> entries = new ArrayList<Range>();
		for (RangeFacet.Range entry : facet.getRanges()) {
			entries.add(new Range(entry.getFrom() == Double.NEGATIVE_INFINITY ? null : entry.getFrom(), entry.getTo() == Double.POSITIVE_INFINITY ? null : entry.getTo(), entry.getCount(), entry.getTotal(), entry.getTotalCount(), entry.getMin(), entry.getMax()));
		}
		return new RangeResult(facet.getName(), entries);
	}

	private static FacetResult parseStatistical(StatisticalFacet facet) {
		return new StatisticalResult(facet.getName(), facet.getCount(), facet.getMax(), facet.getMin(), facet.getMean(), facet.getStdDeviation(), facet.getSumOfSquares(), facet.getTotal(), facet.getVariance());
	}

	private static FacetResult parseHistogram(HistogramFacet facet) {
		List<IntervalUnit> entries = new ArrayList<IntervalUnit>();
		for (HistogramFacet.Histogram entry : facet.getHistograms()) {
			entries.add(new IntervalUnit(entry.getKey(), entry.getCount(), 0, 0, 0, 0, 0));
		}
		return new HistogramResult(facet.getName(), entries);
	}
}
