package com.github.vanroy.springdata.jest;

import static com.github.vanroy.springdata.jest.MappingBuilder.*;
import static org.elasticsearch.index.VersionType.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.springframework.util.CollectionUtils.isEmpty;
import static org.springframework.util.StringUtils.*;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.function.Supplier;

import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import com.github.vanroy.springdata.jest.aggregation.impl.AggregatedPageImpl;
import com.github.vanroy.springdata.jest.internal.ExtendedSearchResult;
import com.github.vanroy.springdata.jest.internal.MultiDocumentResult;
import com.github.vanroy.springdata.jest.internal.SearchScrollResult;
import com.github.vanroy.springdata.jest.mapper.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.Refresh;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.GetAliases;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.aliases.RemoveAliasMapping;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.indices.settings.GetSettings;
import io.searchbox.indices.type.TypeExist;
import io.searchbox.params.Parameters;
import io.searchbox.params.SearchType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.ElasticsearchException;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Mapping;
import org.springframework.data.elasticsearch.annotations.Setting;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentProperty;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;

/**
 * Jest implementation of ElasticsearchOperations.
 *
 * @author Julien Roy
 */
public class JestElasticsearchTemplate implements ElasticsearchOperations, ApplicationContextAware {

	private static final Logger logger = LoggerFactory.getLogger(JestElasticsearchTemplate.class);

	private final JestClient client;
	private final ElasticsearchConverter elasticsearchConverter;
	private final JestResultsMapper resultsMapper;
	private final ErrorMapper errorMapper;
	private final Supplier<SearchSourceBuilder> searchSourceBuilderProvider;

	public JestElasticsearchTemplate(JestClient client) {
		this(client, null, null, null, null);
	}

	public JestElasticsearchTemplate(JestClient client, JestResultsMapper resultMapper) {
		this(client, null, resultMapper);
	}

	public JestElasticsearchTemplate(JestClient client, JestResultsMapper resultMapper, ErrorMapper errorMapper) {
		this(client, null, resultMapper, errorMapper, null);
	}

	public JestElasticsearchTemplate(JestClient client, ErrorMapper errorMapper) {
		this(client, null, null, errorMapper, null);
	}

	public JestElasticsearchTemplate(JestClient client, ElasticsearchConverter elasticsearchConverter, JestResultsMapper resultsMapper) {
		this(client, elasticsearchConverter, resultsMapper, null, null);
	}

	public JestElasticsearchTemplate(JestClient client, ElasticsearchConverter elasticsearchConverter, JestResultsMapper resultsMapper, ErrorMapper errorMapper, Supplier<SearchSourceBuilder> searchSourceBuilderProvider) {
		this.client = client;
		this.elasticsearchConverter = (elasticsearchConverter == null) ? new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext()) : elasticsearchConverter;
		this.resultsMapper = (resultsMapper == null) ? new DefaultJestResultsMapper(this.elasticsearchConverter.getMappingContext()) : resultsMapper;
		this.errorMapper = (errorMapper == null) ? new DefaultErrorMapper() : errorMapper;
		this.searchSourceBuilderProvider = (searchSourceBuilderProvider == null) ? SearchSourceBuilder::new : searchSourceBuilderProvider;
	}

	public static String readFileFromClasspath(String url) {
		StringBuilder stringBuilder = new StringBuilder();

		BufferedReader bufferedReader = null;

		try {
			ClassPathResource classPathResource = new ClassPathResource(url);
			InputStreamReader inputStreamReader = new InputStreamReader(classPathResource.getInputStream());
			bufferedReader = new BufferedReader(inputStreamReader);
			String line;

			String lineSeparator = System.getProperty("line.separator");
			while ((line = bufferedReader.readLine()) != null) {
				stringBuilder.append(line).append(lineSeparator);
			}
		} catch (Exception e) {
			logger.debug(String.format("Failed to load file from url: %s: %s", url, e.getMessage()));
			return null;
		} finally {
			if (bufferedReader != null)
				try {
					bufferedReader.close();
				} catch (IOException e) {
					logger.debug(String.format("Unable to close buffered reader.. %s", e.getMessage()));
				}
		}

		return stringBuilder.toString();
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		if (elasticsearchConverter instanceof ApplicationContextAware) {
			((ApplicationContextAware) elasticsearchConverter).setApplicationContext(context);
		}
	}

	@Override
	public ElasticsearchConverter getElasticsearchConverter() {
		return elasticsearchConverter;
	}

	@Override
	public <T> boolean createIndex(Class<T> clazz) {
		return createIndexIfNotCreated(clazz);
	}

	@Override
	public boolean createIndex(String indexName) {
		return executeWithAcknowledge(new CreateIndex.Builder(indexName).build());
	}

	@Override
	public boolean createIndex(String indexName, Object settings) {

		CreateIndex.Builder createIndexBuilder = new CreateIndex.Builder(indexName);

		if (settings instanceof String) {
			createIndexBuilder.payload(String.valueOf(settings));
		} else if (settings instanceof Map) {
			createIndexBuilder.payload((Map) settings);
		}

		return executeWithAcknowledge(createIndexBuilder.build());
	}

	@Override
	public <T> boolean createIndex(Class<T> clazz, Object settings) {
		return createIndex(getPersistentEntityFor(clazz).getIndexName(), settings);
	}

	@Override
	public <T> boolean putMapping(Class<T> clazz) {
		return putMapping(clazz, buildMappingForClass(clazz));
	}

	@Override
	public <T> boolean putMapping(String indexName, String type, Class<T> clazz) {
		return putMapping(indexName, type, buildMappingForClass(clazz));
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean putMapping(String indexName, String type, Object mapping) {
		Assert.notNull(indexName, "No index defined for putMapping()");
		Assert.notNull(type, "No type defined for putMapping()");

		try {
			Object source = null;
			if (mapping instanceof String) {
				source = String.valueOf(mapping);
			} else if (mapping instanceof Map) {
				XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
				builder.map((Map) mapping);
				source = xContentBuilderToString(builder);
			} else if (mapping instanceof XContentBuilder) {
				source = xContentBuilderToString(((XContentBuilder) mapping));
			} else if (mapping instanceof DocumentMapper) {
				source = ((DocumentMapper) mapping).mappingSource().toString();
			}

			PutMapping.Builder requestBuilder = new PutMapping.Builder(indexName, type, source);

			return executeWithAcknowledge(requestBuilder.build());
		} catch (Exception e) {
			throw new ElasticsearchException("Failed to build mapping for " + indexName + ":" + type, e);
		}
	}

	private <T> String buildMappingForClass(Class<T> clazz) {
		if (clazz.isAnnotationPresent(Mapping.class)) {
			String mappingPath = clazz.getAnnotation(Mapping.class).mappingPath();
			if (hasText(mappingPath)) {
				String mappings = readFileFromClasspath(mappingPath);
				if (hasText(mappings)) {
					return mappings;
				}
			} else {
				logger.info("mappingPath in @Mapping has to be defined. Building mappings using @Field");
			}
		}
		ElasticsearchPersistentEntity<Object> persistentEntity = getPersistentEntityFor(clazz);

		try {

			ElasticsearchPersistentProperty idProperty = persistentEntity.getIdProperty();
			if (idProperty == null) {
				throw new IllegalArgumentException(String.format("No Id property for %s found", clazz.getSimpleName()));
			}

			return xContentBuilderToString(buildMapping(
					clazz,
					persistentEntity.getIndexType(),
					idProperty.getFieldName(),
					persistentEntity.getParentType()
			));
		} catch (Exception e) {
			throw new ElasticsearchException("Failed to build mapping for " + clazz.getSimpleName(), e);
		}
	}
	private String xContentBuilderToString(XContentBuilder builder) {
		builder.close();
		ByteArrayOutputStream bos = (ByteArrayOutputStream) builder.getOutputStream();
		return bos.toString();
	}

	@Override
	public <T> boolean putMapping(Class<T> clazz, Object mapping) {
		return putMapping(getPersistentEntityFor(clazz).getIndexName(), getPersistentEntityFor(clazz).getIndexType(), mapping);
	}

	@Override
	public <T> Map getMapping(Class<T> clazz) {
		return getMapping(getPersistentEntityFor(clazz).getIndexName(), getPersistentEntityFor(clazz).getIndexType());
	}

	@Override
	public Map getMapping(String indexName, String type) {
		Assert.notNull(indexName, "No index defined for putMapping()");
		Assert.notNull(type, "No type defined for putMapping()");
		Map mappings = null;
		try {

			GetMapping.Builder getMappingBuilder = new GetMapping.Builder();
			getMappingBuilder.addIndex(indexName).addType(type);

			JestResult result = execute(getMappingBuilder.build());

			if (!result.getJsonObject().has(indexName)) {
				logger.info("Index {} did not exist when retrieving mappings for type {}.", indexName, type);
			} else {
				JsonObject index = result.getJsonObject().get(indexName).getAsJsonObject();
				if (index != null) {
					JsonObject mappingElem = index.get("mappings").getAsJsonObject();
					if (!mappingElem.has(type)) {
						logger.info("Type {} did not exist in index {} when retrieving mappings.", type, indexName);
					} else {
						mappings = resultsMapper.getEntityMapper().mapToObject(mappingElem.get(type).toString(), Map.class);
					}
				}
			}
		} catch (Exception e) {
			throw new ElasticsearchException("Error while getting mapping for indexName : " + indexName + " type : " + type + " " + e.getMessage());
		}
		return mappings;
	}

	@Override
	public Map<String, Object> getSetting(String indexName) {
		Assert.notNull(indexName, "No index defined for getSettings");

		GetSettings.Builder getSettingsBuilder = new GetSettings.Builder();
		getSettingsBuilder.addIndex(indexName);

		JestResult result = execute(getSettingsBuilder.build());

		JsonObject entries = result.getJsonObject()
				.get(indexName).getAsJsonObject()
				.get("settings").getAsJsonObject()
				.get("index").getAsJsonObject();

		HashMap<String, Object> mappings = new HashMap<>();

		flatMap("index", entries, mappings);

		return mappings;
	}

	private void flatMap(String prefix, JsonObject jsonObject, Map<String, Object> mappings) {
		Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();

		for (Map.Entry<String, JsonElement> entry : entries) {

			String key = entry.getKey();
			JsonElement value = entry.getValue();
			if (value.isJsonPrimitive()) {
				mappings.put(prefix + "." + key, value.getAsString());
			} else if (value.isJsonObject()) {
				flatMap(prefix + "." + key, value.getAsJsonObject(), mappings);
			}
		}
	}

	@Override
	public <T> Map<String, Object> getSetting(Class<T> clazz) {
		return getSetting(getPersistentEntityFor(clazz).getIndexName());
	}

	@Override
	public <T> T queryForObject(GetQuery query, Class<T> clazz) {
		return queryForObject(query, clazz, resultsMapper);
	}

	@Override
	public <T> T queryForObject(GetQuery query, Class<T> clazz, GetResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	public <T> T queryForObject(GetQuery query, Class<T> clazz, JestGetResultMapper mapper) {
		return queryForObject(null, query, clazz, mapper);
	}

	public <T> T queryForObject(String indexName, GetQuery query, Class<T> clazz) {
		return queryForObject(indexName, query, clazz, resultsMapper);
	}

	public <T> T queryForObject(String indexName, GetQuery query, Class<T> clazz, JestGetResultMapper mapper) {

		ElasticsearchPersistentEntity<Object> persistentEntity = getPersistentEntityFor(clazz);

		String index = indexName == null ? persistentEntity.getIndexName() : indexName;

		Get.Builder build = new Get.Builder(index, query.getId()).type(persistentEntity.getIndexType());

		DocumentResult result = execute(build.build(), true);

		return mapper.mapResult(result, clazz);
	}

	@Override
	public <T> T queryForObject(CriteriaQuery query, Class<T> clazz) {
		Page<T> page = queryForPage(query, clazz);
		Assert.isTrue(page.getTotalElements() < 2, "Expected 1 but found " + page.getTotalElements() + " results");
		return page.getTotalElements() > 0 ? page.getContent().get(0) : null;
	}

	@Override
	public <T> T queryForObject(StringQuery query, Class<T> clazz) {
		Page<T> page = queryForPage(query, clazz);
		Assert.isTrue(page.getTotalElements() < 2, "Expected 1 but found " + page.getTotalElements() + " results");
		return page.getTotalElements() > 0 ? page.getContent().get(0) : null;
	}

	@Override
	public <T> AggregatedPage<T> queryForPage(SearchQuery query, Class<T> clazz) {
		return queryForPage(query, clazz, resultsMapper);
	}

	@Override
	public <T> AggregatedPage<T> queryForPage(SearchQuery query, Class<T> clazz, SearchResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> List<Page<T>> queryForPage(List<SearchQuery> queries, Class<T> clazz) {
		return queryForPage(queries, clazz, resultsMapper);
	}

	@Override
	public <T> List<Page<T>> queryForPage(List<SearchQuery> queries, Class<T> clazz, SearchResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	public <T> List<Page<T>> queryForPage(List<SearchQuery> queries, Class<T> clazz, JestSearchResultMapper mapper) {
		List<Search> searches = new ArrayList<>();
		for (SearchQuery query : queries) {
			searches.add(prepareSearch(prepareSearch(query, clazz), query));
		}
		MultiSearch request = new MultiSearch.Builder(searches).build();
		return doMultiSearch(queries, clazz, request, mapper);
	}

	@Override
	public List<Page<?>> queryForPage(List<SearchQuery> queries, List<Class<?>> classes) {
		return queryForPage(queries, classes, resultsMapper);
	}

	@Override
	public List<Page<?>> queryForPage(List<SearchQuery> queries, List<Class<?>> classes, SearchResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	public List<Page<?>> queryForPage(List<SearchQuery> queries, List<Class<?>> classes, JestSearchResultMapper mapper) {
		Assert.isTrue(queries.size() == classes.size(), "Queries should have same length with classes");
		List<Search> searches = new ArrayList<>();
		Iterator<Class<?>> it = classes.iterator();
		for (SearchQuery query : queries) {
			searches.add(prepareSearch(prepareSearch(query, it.next()), query));
		}
		MultiSearch request = new MultiSearch.Builder(searches).build();
		return doMultiSearch(queries, classes, request, mapper);
	}

	private <T> List<Page<T>> doMultiSearch(List<SearchQuery> queries, Class<T> clazz, MultiSearch request, JestSearchResultMapper resultsMapper) {
		List<MultiSearchResult.MultiSearchResponse> results = executeMultiSearch(request).getResponses();
		List<Page<T>> res = new ArrayList<>(queries.size());
		int c = 0;
		for (SearchQuery query : queries) {
			res.add(resultsMapper.mapResults(new ExtendedSearchResult(results.get(c++).searchResult), clazz, query.getPageable()));
		}
		return res;
	}

	private List<Page<?>> doMultiSearch(List<SearchQuery> queries, List<Class<?>> classes, MultiSearch request, JestSearchResultMapper resultsMapper) {
		List<MultiSearchResult.MultiSearchResponse> results = executeMultiSearch(request).getResponses();
		List<Page<?>> res = new ArrayList<>(queries.size());
		int c = 0;
		Iterator<Class<?>> it = classes.iterator();
		for (SearchQuery query : queries) {
			res.add(resultsMapper.mapResults(new ExtendedSearchResult(results.get(c++).searchResult), it.next(), query.getPageable()));
		}
		return res;
	}

	public <T> AggregatedPage<T> queryForPage(SearchQuery query, Class<T> clazz, JestSearchResultMapper mapper) {
		SearchResult response = doSearch(prepareSearch(query, clazz), query);
		return mapper.mapResults(response, clazz, query.getAggregations(), query.getPageable());
	}

	@Override
	public <T> T query(SearchQuery query, ResultsExtractor<T> resultsExtractor) {
		throw new UnsupportedOperationException();
	}

	public <T> T query(SearchQuery query, JestResultsExtractor<T> resultsExtractor) {
		SearchResult response = doSearch(prepareSearch(query), query);
		return resultsExtractor.extract(response);
	}

	@Override
	public <T> List<T> queryForList(CriteriaQuery query, Class<T> clazz) {
		return queryForPage(query, clazz).getContent();
	}

	@Override
	public <T> List<T> queryForList(StringQuery query, Class<T> clazz) {
		return queryForPage(query, clazz).getContent();
	}

	@Override
	public <T> List<T> queryForList(SearchQuery query, Class<T> clazz) {
		return queryForPage(query, clazz).getContent();
	}

	@Override
	public <T> List<String> queryForIds(SearchQuery query) {
		SearchSourceBuilder search = prepareSearch(query).query(query.getQuery()).fetchSource(false);
		if (query.getFilter() != null) {
			search.postFilter(query.getFilter());
		}

		SearchResult result = executeSearch(query, search);
		return extractIds(result);
	}

	@Override
	public <T> Page<T> queryForPage(CriteriaQuery criteriaQuery, Class<T> clazz) {
		QueryBuilder elasticsearchQuery = new CriteriaQueryProcessor().createQueryFromCriteria(criteriaQuery.getCriteria());
		QueryBuilder elasticsearchFilter = new CriteriaFilterProcessor().createFilterFromCriteria(criteriaQuery.getCriteria());

		SearchSourceBuilder searchRequestBuilder = prepareSearch(criteriaQuery, clazz);

		if (elasticsearchQuery != null) {
			searchRequestBuilder.query(elasticsearchQuery);
		} else {
			searchRequestBuilder.query(QueryBuilders.matchAllQuery());
		}

		if (criteriaQuery.getMinScore() > 0) {
			searchRequestBuilder.minScore(criteriaQuery.getMinScore());
		}

		if (elasticsearchFilter != null)
			searchRequestBuilder.postFilter(elasticsearchFilter);

		SearchResult response = executeSearch(criteriaQuery, searchRequestBuilder);
		return resultsMapper.mapResults(response, clazz, criteriaQuery.getPageable());
	}

	@Override
	public <T> Page<T> queryForPage(StringQuery query, Class<T> clazz) {
		return queryForPage(query, clazz, resultsMapper);
	}

	@Override
	public <T> Page<T> queryForPage(StringQuery query, Class<T> clazz, SearchResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	public <T> Page<T> queryForPage(StringQuery query, Class<T> clazz, JestSearchResultMapper mapper) {
		SearchResult response = executeSearch(query, prepareSearch(query, clazz).query(wrapperQuery(query.getSource())));
		return mapper.mapResults(response, clazz, query.getPageable());
	}


	@Override
	public <T> CloseableIterator<T> stream(CriteriaQuery query, Class<T> clazz) {
		final long scrollTimeInMillis = TimeValue.timeValueMinutes(1).millis();
		return doStream(scrollTimeInMillis, (ScrolledPage<T>) startScroll(scrollTimeInMillis, query, clazz), clazz, resultsMapper);
	}

	@Override
	public <T> CloseableIterator<T> stream(SearchQuery query, Class<T> clazz) {
		return stream(query, clazz, resultsMapper);
	}

	@Override
	public <T> CloseableIterator<T> stream(SearchQuery query, final Class<T> clazz, final SearchResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	public <T> CloseableIterator<T> stream(SearchQuery query, final Class<T> clazz, final JestResultsMapper mapper) {
		final long scrollTimeInMillis = TimeValue.timeValueMinutes(1).millis();
		return doStream(scrollTimeInMillis, (ScrolledPage<T>) startScroll(scrollTimeInMillis, query, clazz, mapper), clazz, mapper);
	}

	private <T> CloseableIterator<T> doStream(final long scrollTimeInMillis, final ScrolledPage<T> page, final Class<T> clazz, final JestResultsMapper mapper) {
		return new CloseableIterator<T>() {

			/** As we couldn't retrieve single result with scroll, store current hits. */
			private volatile Iterator<T> currentHits = page.iterator();

			/** The scroll id. */
			private volatile String scrollId = page.getScrollId();

			/** If stream is finished (ie: cluster returns no results. */
			private volatile boolean finished = !currentHits.hasNext();

			@Override
			public void close() {
				try {
					// Clear scroll on cluster only in case of error (cause elasticsearch auto clear scroll when it's done)
					if (!finished && scrollId != null && currentHits != null && currentHits.hasNext()) {
						clearScroll(scrollId);
					}
				} finally {
					currentHits = null;
					scrollId = null;
				}
			}

			@Override
			public boolean hasNext() {
				// Test if stream is finished
				if (finished) {
					return false;
				}
				// Test if it remains hits
				if (currentHits == null || !currentHits.hasNext()) {
					// Do a new request
					final ScrolledPage<T> scroll = (ScrolledPage<T>) continueScroll(scrollId, scrollTimeInMillis, clazz, mapper);
					// Save hits and scroll id
					currentHits = scroll.iterator();
					finished = !currentHits.hasNext();
					scrollId = scroll.getScrollId();
				}
				return currentHits.hasNext();
			}

			@Override
			public T next() {
				if (hasNext()) {
					return currentHits.next();
				}
				throw new NoSuchElementException();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("remove");
			}
		};
	}

	@Override
	public <T> long count(CriteriaQuery criteriaQuery, Class<T> clazz) {
		QueryBuilder elasticsearchQuery = new CriteriaQueryProcessor().createQueryFromCriteria(criteriaQuery.getCriteria());
		QueryBuilder elasticsearchFilter = new CriteriaFilterProcessor().createFilterFromCriteria(criteriaQuery.getCriteria());

		if (elasticsearchFilter == null) {
			return doCount(prepareCount(criteriaQuery, clazz), elasticsearchQuery);
		} else {
			// filter could not be set into CountRequestBuilder, convert request into search request
			return doCount(prepareSearch(criteriaQuery, clazz), elasticsearchQuery, elasticsearchFilter);
		}
	}

	@Override
	public <T> long count(SearchQuery searchQuery, Class<T> clazz) {
		QueryBuilder elasticsearchQuery = searchQuery.getQuery();
		QueryBuilder elasticsearchFilter = searchQuery.getFilter();

		if (elasticsearchFilter == null) {
			return doCount(prepareCount(searchQuery, clazz), elasticsearchQuery);
		} else {
			// filter could not be set into CountRequestBuilder, convert request into search request
			return doCount(prepareSearch(searchQuery, clazz), elasticsearchQuery, elasticsearchFilter);
		}
	}

	@Override
	public <T> long count(CriteriaQuery query) {
		return count(query, null);
	}

	@Override
	public <T> long count(SearchQuery query) {
		return count(query, null);
	}

	private long doCount(Count.Builder countRequestBuilder, QueryBuilder elasticsearchQuery) {
		if (elasticsearchQuery != null) {
			countRequestBuilder.query(searchSourceBuilderProvider.get().query(elasticsearchQuery).toString());
		}

		CountResult result = execute(countRequestBuilder.build());
		return result.getCount().longValue();
	}

	private long doCount(SearchSourceBuilder searchRequestBuilder, QueryBuilder elasticsearchQuery, QueryBuilder elasticsearchFilter) {
		if (elasticsearchQuery != null) {
			searchRequestBuilder.query(elasticsearchQuery);
		} else {
			searchRequestBuilder.query(QueryBuilders.matchAllQuery());
		}
		if (elasticsearchFilter != null) {
			searchRequestBuilder.postFilter(elasticsearchFilter);
		}

		CountResult result = execute(new Count.Builder().query(searchRequestBuilder.toString()).build());
		return result.getCount().longValue();
	}

	private <T> Count.Builder prepareCount(Query query, Class<T> clazz) {
		String indexName[] = !isEmpty(query.getIndices()) ? query.getIndices().toArray(new String[query.getIndices().size()]) : retrieveIndexNameFromPersistentEntity(clazz);
		String types[] = !isEmpty(query.getTypes()) ? query.getTypes().toArray(new String[query.getTypes().size()]) : retrieveTypeFromPersistentEntity(clazz);

		Assert.notNull(indexName, "No index defined for Query");

		Count.Builder countRequestBuilder = new Count.Builder().addIndices(Arrays.asList(indexName));
		if (types != null) {
			countRequestBuilder.addTypes(Arrays.asList(types));
		}
		return countRequestBuilder;
	}

	@Override
	public <T> LinkedList<T> multiGet(SearchQuery searchQuery, Class<T> clazz, MultiGetResultMapper getResultMapper) {
		throw new UnsupportedOperationException();
	}

	public <T> LinkedList<T> multiGet(SearchQuery searchQuery, Class<T> clazz, JestMultiGetResultMapper getResultMapper) {
		return getResultMapper.mapResults(getMultiResponse(searchQuery, clazz), clazz);
	}

	@Override
	public <T> LinkedList<T> multiGet(SearchQuery searchQuery, Class<T> clazz) {
		return resultsMapper.mapResults(getMultiResponse(searchQuery, clazz), clazz);
	}

	private <T> MultiDocumentResult getMultiResponse(Query searchQuery, Class<T> clazz) {

		String indexName = !isEmpty(searchQuery.getIndices()) ? searchQuery.getIndices().get(0) : getPersistentEntityFor(clazz).getIndexName();
		String type = !isEmpty(searchQuery.getTypes()) ? searchQuery.getTypes().get(0) : getPersistentEntityFor(clazz).getIndexType();

		Assert.notNull(indexName, "No index defined for Query");
		Assert.notNull(type, "No type define for Query");
		Assert.notEmpty(searchQuery.getIds(), "No Id define for Query");

		MultiGet.Builder.ById builder = new MultiGet.Builder.ById(indexName, type).addId(searchQuery.getIds());

		return new MultiDocumentResult(execute(builder.build()));
	}

	@Override
	public String index(IndexQuery query) {

		String documentId = execute(prepareIndex(query)).getId();

		// We should call this because we are not going through a mapper.
		if (query.getObject() != null && isDocument(query.getObject().getClass())) {
			setPersistentEntityId(query.getObject(), documentId);
		}
		return documentId;
	}

	@Override
	public UpdateResponse update(UpdateQuery updateQuery) {

		DocumentResult result = execute(prepareUpdate(updateQuery));

		return new UpdateResponse(ShardId.fromString("[][0]"), result.getType(), result.getId(), result.getJsonObject().get("_version").getAsLong(), DocWriteResponse.Result.CREATED);
	}

	@Override
	public void bulkIndex(List<IndexQuery> queries, BulkOptions bulkOptions) {
		Bulk.Builder bulk = new Bulk.Builder();

		setBulkOptions(bulk, bulkOptions);

		for (IndexQuery query : queries) {
			bulk.addAction(prepareIndex(query));
		}

		BulkResult bulkResult = new BulkResult(execute(bulk.build()));
		if (!bulkResult.isSucceeded()) {
			Map<String, String> failedDocuments = new HashMap<>();
			for (BulkResult.BulkResultItem item : bulkResult.getFailedItems()) {
				failedDocuments.put(item.id, item.error);
			}
			throw new ElasticsearchException(
					"Bulk indexing has failures. Use ElasticsearchException.getFailedDocuments() for detailed messages ["
							+ failedDocuments + "]", failedDocuments
			);
		}
	}

	@Override
	public void bulkUpdate(List<UpdateQuery> queries, BulkOptions bulkOptions) {

		Bulk.Builder bulk = new Bulk.Builder();

		setBulkOptions(bulk, bulkOptions);

		for (UpdateQuery query : queries) {
			bulk.addAction(prepareUpdate(query));
		}

		BulkResult bulkResult = new BulkResult(execute(bulk.build()));
		if (!bulkResult.isSucceeded()) {
			Map<String, String> failedDocuments = new HashMap<>();
			for (BulkResult.BulkResultItem item : bulkResult.getFailedItems()) {
				failedDocuments.put(item.id, item.error);
			}
			throw new ElasticsearchException(
					"Bulk indexing has failures. Use ElasticsearchException.getFailedDocuments() for detailed messages ["
							+ failedDocuments + "]", failedDocuments
			);
		}
	}

	private static void setBulkOptions(Bulk.Builder bulkRequest, BulkOptions bulkOptions) {

		if (bulkOptions.getTimeout() != null) {
			bulkRequest.setParameter("timeout", bulkOptions.getTimeout());
		}

		if (bulkOptions.getRefreshPolicy() != null) {
			bulkRequest.setParameter("refresh_policy", bulkOptions.getRefreshPolicy());
		}

		if (bulkOptions.getWaitForActiveShards() != null) {
			bulkRequest.setParameter("wait_for_active_shards", bulkOptions.getWaitForActiveShards());
		}

		if (bulkOptions.getPipeline() != null) {
			bulkRequest.setParameter("pipeline", bulkOptions.getPipeline());
		}

		if (bulkOptions.getRoutingId() != null) {
			bulkRequest.setParameter("routing_id", bulkOptions.getRoutingId());
		}

	}

	@Override
	public String delete(String indexName, String type, String id) {
		return execute(new Delete.Builder(id).index(indexName).type(type).build(), true).getId();
	}

	@Override
	public <T> void delete(CriteriaQuery criteriaQuery, Class<T> clazz) {
		QueryBuilder elasticsearchQuery = new CriteriaQueryProcessor().createQueryFromCriteria(criteriaQuery.getCriteria());
		Assert.notNull(elasticsearchQuery, "Query can not be null.");
		DeleteQuery deleteQuery = new DeleteQuery();
		deleteQuery.setQuery(elasticsearchQuery);
		delete(deleteQuery, clazz);
	}

	@Override
	public <T> String delete(Class<T> clazz, String id) {
		ElasticsearchPersistentEntity persistentEntity = getPersistentEntityFor(clazz);
		return delete(persistentEntity.getIndexName(), persistentEntity.getIndexType(), id);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> void delete(DeleteQuery deleteQuery, Class<T> clazz) {

		String indexName = hasText(deleteQuery.getIndex()) ? deleteQuery.getIndex() : getPersistentEntityFor(clazz).getIndexName();
		String typeName = hasText(deleteQuery.getType()) ? deleteQuery.getType() : getPersistentEntityFor(clazz).getIndexType();
		Integer pageSize = deleteQuery.getPageSize() != null ? deleteQuery.getPageSize() : 1000;
		Long scrollTimeInMillis = deleteQuery.getScrollTimeInMillis() != null ? deleteQuery.getScrollTimeInMillis() : 10000L;

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(deleteQuery.getQuery())
				.withIndices(indexName)
				.withTypes(typeName)
				.withPageable(PageRequest.of(0, pageSize))
				.build();

		JestSearchResultMapper onlyIdSearchResultMapper = new JestSearchResultMapper() {
			@Override
			public <U> AggregatedPage<U> mapResults(SearchResult response, Class<U> clazz, Pageable pageable) {
				List<String> result = new ArrayList<>();

				for (SearchResult.Hit<JsonObject, Void> searchHit : response.getHits(JsonObject.class)) {
					result.add(searchHit.source.get(JestResult.ES_METADATA_ID).getAsString());
				}

				if (result.size() > 0) {
					return new AggregatedPageImpl<U>((List<U>) result, ((ExtendedSearchResult) response).getScrollId());
				}
				return new AggregatedPageImpl<U>(Collections.emptyList(), ((ExtendedSearchResult) response).getScrollId());
			}

			@Override
			public <U> AggregatedPage<U> mapResults(SearchResult response, Class<U> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable) {
				return mapResults(response, clazz, pageable);
			}
		};

		Page<String> scrolledResult = startScroll(scrollTimeInMillis, searchQuery, String.class, onlyIdSearchResultMapper);
		List<String> ids = new ArrayList<>();

		JestScrollResultMapper onlyIdResultMapper = new JestScrollResultMapper() {
			@Override
			public <U> ScrolledPage<U> mapResults(SearchScrollResult response, Class<U> clazz) {
				List<String> result = new ArrayList<>();

				for (SearchScrollResult.Hit<JsonObject, Void> searchHit : response.getHits(JsonObject.class)) {
					result.add(searchHit.source.get(JestResult.ES_METADATA_ID).getAsString());
				}

				if (result.isEmpty()) {
					return new AggregatedPageImpl<>(Collections.emptyList(), response.getScrollId());
				}
				return new AggregatedPageImpl<>((List<U>) result, response.getScrollId());
			}
		};

		do {
			ids.addAll(scrolledResult.getContent());
			scrolledResult = continueScroll(((ScrolledPage<T>) scrolledResult).getScrollId(), scrollTimeInMillis, String.class, onlyIdResultMapper);
		} while (scrolledResult.getContent().size() != 0);

		if (!ids.isEmpty()) {

			Bulk.Builder bulk = new Bulk.Builder();
			for (String id : ids) {
				bulk.addAction(new Delete.Builder(id).index(indexName).type(typeName).build());
			}
			execute(bulk.build());
		}

		clearScroll(((ScrolledPage) scrolledResult).getScrollId());
	}

	@Override
	public void delete(DeleteQuery deleteQuery) {
		Assert.notNull(deleteQuery.getIndex(), "No index defined for Query");
		Assert.notNull(deleteQuery.getType(), "No type define for Query");
		delete(deleteQuery, null);
	}

	@Override
	public <T> boolean deleteIndex(Class<T> clazz) {
		return deleteIndex(getPersistentEntityFor(clazz).getIndexName());
	}

	@Override
	public boolean deleteIndex(String indexName) {
		Assert.notNull(indexName, "No index defined for delete operation");
		return indexExists(indexName) && executeWithAcknowledge(new DeleteIndex.Builder(indexName).build());
	}

	@Override
	public <T> boolean indexExists(Class<T> clazz) {
		return indexExists(getPersistentEntityFor(clazz).getIndexName());
	}

	@Override
	public boolean indexExists(String indexName) {
		return executeWithAcknowledge(new IndicesExists.Builder(indexName).build());
	}

	@Override
	public boolean typeExists(String index, String type) {
		return executeWithAcknowledge(new TypeExist.Builder(index).addType(type).build());
	}

	@Override
	public void refresh(String indexName) {
		execute(new Refresh.Builder().addIndex(indexName).build());
	}

	@Override
	public <T> void refresh(Class<T> clazz) {
		ElasticsearchPersistentEntity persistentEntity = getPersistentEntityFor(clazz);
		execute(new Refresh.Builder().addIndex(persistentEntity.getIndexName()).build());
	}

	private <T> SearchSourceBuilder prepareScroll(Query query, Class<T> clazz) {
		setPersistentEntityIndexAndType(query, clazz);
		return prepareScroll(query);
	}

	private SearchSourceBuilder prepareScroll(Query query) {

		SearchSourceBuilder searchSourceBuilder = searchSourceBuilderProvider.get();

		if (query.getPageable() != null && query.getPageable().isPaged()) {
			searchSourceBuilder.size(query.getPageable().getPageSize());
		}
		searchSourceBuilder.from(0);

		if (!isEmpty(query.getFields())) {
			searchSourceBuilder.fetchSource(toArray(query.getFields()), null);
		}

		if (query.getSort() != null) {
			for (Sort.Order order : query.getSort()) {
				searchSourceBuilder.sort(order.getProperty(), order.getDirection() == Sort.Direction.DESC ? SortOrder.DESC : SortOrder.ASC);
			}
		}

		if (query.getMinScore() > 0) {
			searchSourceBuilder.minScore(query.getMinScore());
		}

		return searchSourceBuilder;
	}

	private SearchResult doScroll(SearchSourceBuilder searchSourceBuilder, CriteriaQuery criteriaQuery, long scrollTimeInMillis) {
		Assert.notNull(criteriaQuery.getIndices(), "No index defined for Query");
		Assert.notNull(criteriaQuery.getTypes(), "No type define for Query");
		Assert.notNull(criteriaQuery.getPageable(), "Query.pageable is required for scan & scroll");

		QueryBuilder elasticsearchQuery = new CriteriaQueryProcessor().createQueryFromCriteria(criteriaQuery.getCriteria());
		QueryBuilder elasticsearchFilter = new CriteriaFilterProcessor().createFilterFromCriteria(criteriaQuery.getCriteria());

		searchSourceBuilder.query(elasticsearchQuery != null ? elasticsearchQuery : QueryBuilders.matchAllQuery());

		if (elasticsearchFilter != null) {
			searchSourceBuilder.postFilter(elasticsearchFilter);
		}

		Search.Builder search = new Search.Builder(searchSourceBuilder.toString()).
				addTypes(criteriaQuery.getTypes()).
				addIndices(criteriaQuery.getIndices()).
				setParameter(Parameters.SIZE, criteriaQuery.getPageable().getPageSize()).
				setParameter(Parameters.SCROLL, scrollTimeInMillis + "ms");

		return new ExtendedSearchResult(execute(search.build()));
	}

	private SearchResult doScroll(SearchSourceBuilder searchSourceBuilder, SearchQuery searchQuery, long scrollTimeInMillis) {
		Assert.notNull(searchQuery.getIndices(), "No index defined for Query");
		Assert.notNull(searchQuery.getTypes(), "No type define for Query");
		Assert.notNull(searchQuery.getPageable(), "Query.pageable is required for scan & scroll");

		QueryBuilder elasticsearchQuery = searchQuery.getQuery();
		searchSourceBuilder.query(elasticsearchQuery != null ? elasticsearchQuery : QueryBuilders.matchAllQuery());

		if (searchQuery.getFilter() != null) {
			searchSourceBuilder.postFilter(searchQuery.getFilter());
		}

		Search.Builder search = new Search.Builder(searchSourceBuilder.toString()).
				addTypes(searchQuery.getTypes()).
				addIndices(searchQuery.getIndices()).
				setParameter(Parameters.SIZE, searchQuery.getPageable().getPageSize()).
				setParameter(Parameters.SCROLL, scrollTimeInMillis + "ms");

		return new ExtendedSearchResult(execute(search.build()));
	}

	@Override
	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, SearchQuery searchQuery, Class<T> clazz) {
		SearchResult response = doScroll(prepareScroll(searchQuery, clazz), searchQuery, scrollTimeInMillis);
		return resultsMapper.mapResults(response, clazz, searchQuery.getPageable());
	}

	@Override
	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, CriteriaQuery criteriaQuery, Class<T> clazz) {
		SearchResult response = doScroll(prepareScroll(criteriaQuery, clazz), criteriaQuery, scrollTimeInMillis);
		return resultsMapper.mapResults(response, clazz, criteriaQuery.getPageable());
	}

	@Override
	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, SearchQuery searchQuery, Class<T> clazz, SearchResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, SearchQuery searchQuery, Class<T> clazz, JestSearchResultMapper mapper) {
		SearchResult response = doScroll(prepareScroll(searchQuery, clazz), searchQuery, scrollTimeInMillis);
		return mapper.mapResults(response, clazz, searchQuery.getPageable());
	}

	@Override
	public <T> ScrolledPage<T> startScroll(long scrollTimeInMillis, CriteriaQuery criteriaQuery, Class<T> clazz, SearchResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	public <T> Page<T> startScroll(long scrollTimeInMillis, CriteriaQuery criteriaQuery, Class<T> clazz, JestSearchResultMapper mapper) {
		SearchResult response = doScroll(prepareScroll(criteriaQuery, clazz), criteriaQuery, scrollTimeInMillis);
		return mapper.mapResults(response, clazz, criteriaQuery.getPageable());
	}

	@Override
	public <T> ScrolledPage<T> continueScroll(@Nullable String scrollId, long scrollTimeInMillis, Class<T> clazz) {
		SearchScroll scroll = new SearchScroll.Builder(scrollId, scrollTimeInMillis + "ms").build();
		SearchScrollResult response = new SearchScrollResult(execute(scroll));

		return resultsMapper.mapResults(response, clazz);
	}

	@Override
	public <T> ScrolledPage<T> continueScroll(@Nullable String scrollId, long scrollTimeInMillis, Class<T> clazz, SearchResultMapper mapper) {
		throw new UnsupportedOperationException();
	}

	public <T> Page<T> continueScroll(@Nullable String scrollId, long scrollTimeInMillis, Class<T> clazz, JestScrollResultMapper mapper) {
		SearchScroll scroll = new SearchScroll.Builder(scrollId, scrollTimeInMillis + "ms").build();
		SearchScrollResult response = new SearchScrollResult(execute(scroll));

		return mapper.mapResults(response, clazz);
	}

	@Override
	public void clearScroll(String scrollId) {
		execute(new ClearScroll.Builder().addScrollId(scrollId).build(), true);
	}

	public <T> Page<T> moreLikeThis(MoreLikeThisQuery query, Class<T> clazz) {
		ElasticsearchPersistentEntity persistentEntity = getPersistentEntityFor(clazz);
		String indexName = hasText(query.getIndexName()) ? query.getIndexName() : persistentEntity.getIndexName();
		String type = hasText(query.getType()) ? query.getType() : persistentEntity.getIndexType();

		Assert.notNull(indexName, "No 'indexName' defined for MoreLikeThisQuery");
		Assert.notNull(type, "No 'type' defined for MoreLikeThisQuery");
		Assert.notNull(query.getId(), "No document id defined for MoreLikeThisQuery");

		MoreLikeThisQueryBuilder moreLikeThisQueryBuilder = moreLikeThisQuery(toArray(new MoreLikeThisQueryBuilder.Item(indexName, type, query.getId())));

		if (query.getMinTermFreq() != null) {
			moreLikeThisQueryBuilder.minTermFreq(query.getMinTermFreq());
		}
		if (query.getMaxQueryTerms() != null) {
			moreLikeThisQueryBuilder.maxQueryTerms(query.getMaxQueryTerms());
		}
		if (!isEmpty(query.getStopWords())) {
			moreLikeThisQueryBuilder.stopWords(toArray(query.getStopWords()));
		}
		if (query.getMinDocFreq() != null) {
			moreLikeThisQueryBuilder.minDocFreq(query.getMinDocFreq());
		}
		if (query.getMaxDocFreq() != null) {
			moreLikeThisQueryBuilder.maxDocFreq(query.getMaxDocFreq());
		}
		if (query.getMinWordLen() != null) {
			moreLikeThisQueryBuilder.minWordLength(query.getMinWordLen());
		}
		if (query.getMaxWordLen() != null) {
			moreLikeThisQueryBuilder.maxWordLength(query.getMaxWordLen());
		}
		if (query.getBoostTerms() != null) {
			moreLikeThisQueryBuilder.boostTerms(query.getBoostTerms());
		}

		return queryForPage(new NativeSearchQueryBuilder().withQuery(moreLikeThisQueryBuilder).build(), clazz);
	}

	@Override
	public boolean addAlias(AliasQuery query) {
		Assert.notNull(query.getIndexName(), "No index defined for Alias");
		Assert.notNull(query.getAliasName(), "No alias defined");

		AddAliasMapping.Builder aliasAction = new AddAliasMapping.Builder(query.getIndexName(), query.getAliasName());
		if (query.getFilterBuilder() != null) {
			//TODO(setFilter on alias)
//            aliasAction.setFilter(query.getFilterBuilder());
		} else if (query.getFilter() != null) {
			aliasAction.setFilter(query.getFilter());
		} else if (hasText(query.getRouting())) {
			aliasAction.addRouting(query.getRouting());
		} else if (hasText(query.getSearchRouting())) {
			aliasAction.addSearchRouting(query.getSearchRouting());
		} else if (hasText(query.getIndexRouting())) {
			aliasAction.addIndexRouting(query.getIndexRouting());
		}
		return executeWithAcknowledge(new ModifyAliases.Builder(aliasAction.build()).build());
	}

	@Override
	public boolean removeAlias(AliasQuery query) {
		Assert.notNull(query.getIndexName(), "No index defined for Alias");
		Assert.notNull(query.getAliasName(), "No alias defined");

		RemoveAliasMapping removeAlias = new RemoveAliasMapping.Builder(query.getIndexName(), query.getAliasName()).build();
		return executeWithAcknowledge(new ModifyAliases.Builder(removeAlias).build());
	}

	@Override
	public List<AliasMetaData> queryForAlias(String indexName) {

		GetAliases getAliases = new GetAliases.Builder().addIndex(indexName).build();
		JestResult result = execute(getAliases);
		if (!result.isSucceeded()) {
			return Collections.emptyList();
		}

		Set<Map.Entry<String, JsonElement>> entries = result.getJsonObject().getAsJsonObject(indexName).getAsJsonObject("aliases").entrySet();

		List<AliasMetaData> aliases = new ArrayList<>(entries.size());
		for (Map.Entry<String, JsonElement> entry : entries) {
			aliases.add(AliasMetaData.newAliasMetaDataBuilder(entry.getKey()).build());
		}
		return aliases;
	}

	public Set<String> getIndicesFromAlias(String aliasName) {
		JestResult result = execute(new GetAliases.Builder().addIndex(aliasName).build());
		if (!result.isSucceeded()) {
			return Collections.emptySet();
		}

		Set<Map.Entry<String, JsonElement>> entries = result.getJsonObject().entrySet();
		Set<String> indices = new HashSet<>(entries.size());
		for (Map.Entry<String, JsonElement> entry : entries) {
			indices.add(entry.getKey());
		}
		return indices;
	}

	@SuppressWarnings("unchecked")
	public ElasticsearchPersistentEntity<Object> getPersistentEntityFor(Class clazz) {
		Assert.isTrue(clazz.isAnnotationPresent(Document.class), "Unable to identify index name. " + clazz.getSimpleName()
				+ " is not a Document. Make sure the document class is annotated with @Document(indexName=\"foo\")");
		return (ElasticsearchPersistentEntity<Object>) elasticsearchConverter.getMappingContext().getRequiredPersistentEntity(clazz);
	}

	private <T extends JestResult> T execute(Action<T> action) {
		return execute(action, false);
	}

	private <T extends JestResult> T execute(Action<T> action, boolean acceptNotFound) {
		try {

			// Execute action
			T result = client.execute(action);

			// Check result and map error
			errorMapper.mapError(action, result, acceptNotFound);

			return result;
		} catch (IOException e) {
			throw new ElasticsearchException("failed to execute action", e);
		}
	}

	private boolean executeWithAcknowledge(Action<?> action) {
		return execute(action, true).isSucceeded();
	}

	private <T> SearchSourceBuilder prepareSearch(Query query, Class<T> clazz) {
		setPersistentEntityIndexAndType(query, clazz);
		return prepareSearch(query);
	}

	private SearchSourceBuilder prepareSearch(Query query) {
		Assert.notNull(query.getIndices(), "No index defined for Query");
		Assert.notNull(query.getTypes(), "No type defined for Query");

		SearchSourceBuilder searchSourceBuilder = searchSourceBuilderProvider.get();

		int startRecord = 0;

		if (query.getPageable() != null && query.getPageable().isPaged()) {
			startRecord = (int) query.getPageable().getOffset();
			searchSourceBuilder.size(query.getPageable().getPageSize());
		}
		searchSourceBuilder.from(startRecord);

		if (!query.getFields().isEmpty()) {
			searchSourceBuilder.fetchSource(toArray(query.getFields()), null);
		}

		if (query.getSourceFilter() != null) {
			SourceFilter sourceFilter = query.getSourceFilter();
			searchSourceBuilder.fetchSource(sourceFilter.getIncludes(), sourceFilter.getExcludes());
		}

		if (query.getSort() != null) {
			for (Sort.Order order : query.getSort()) {
				searchSourceBuilder.sort(order.getProperty(), order.getDirection() == Sort.Direction.DESC ? SortOrder.DESC : SortOrder.ASC);
			}
		}

		if (query.getMinScore() > 0) {
			searchSourceBuilder.minScore(query.getMinScore());
		}

		searchSourceBuilder.trackScores(query.getTrackScores());
		return searchSourceBuilder;
	}

	private Search prepareSearch(SearchSourceBuilder searchSourceBuilder, SearchQuery searchQuery) {
		if (searchQuery.getFilter() != null) {
			searchSourceBuilder.postFilter(searchQuery.getFilter());
		}

		if (!isEmpty(searchQuery.getElasticsearchSorts())) {
			for (SortBuilder sort : searchQuery.getElasticsearchSorts()) {
				searchSourceBuilder.sort(sort);
			}
		}

		if (searchQuery.getHighlightFields() != null) {
			HighlightBuilder highlighter = SearchSourceBuilder.highlight();
			for (HighlightBuilder.Field highlightField : searchQuery.getHighlightFields()) {
				highlighter.field(highlightField);
			}
			searchSourceBuilder.highlighter(highlighter);
		}

		if (!isEmpty(searchQuery.getAggregations())) {
			for (AbstractAggregationBuilder aggregationBuilder : searchQuery.getAggregations()) {
				searchSourceBuilder.aggregation(aggregationBuilder);
			}
		}

		if (!isEmpty(searchQuery.getIndicesBoost())) {
			for (IndexBoost indexBoost : searchQuery.getIndicesBoost()) {
				searchSourceBuilder.indexBoost(indexBoost.getIndexName(), indexBoost.getBoost());
			}
		}

		if (!searchQuery.getScriptFields().isEmpty()) {
			for (ScriptField scriptedField : searchQuery.getScriptFields()) {
				searchSourceBuilder.scriptField(scriptedField.fieldName(), scriptedField.script());
			}
		}

		if (searchQuery.getCollapseBuilder() != null) {
			searchSourceBuilder.collapse(searchQuery.getCollapseBuilder());
		}

		SearchSourceBuilder request = searchSourceBuilder.query(searchQuery.getQuery());
		return prepareQuery(request, searchQuery);
	}

	private Search prepareQuery(SearchSourceBuilder request, Query query) {
		Search.Builder search = new Search.Builder(request.toString());
		if (query != null) {
			search.
					addTypes(query.getTypes()).
					addIndices(query.getIndices()).
					setSearchType(SearchType.valueOf(query.getSearchType().name()));
		}
		return search.build();
	}

	private SearchResult doSearch(SearchSourceBuilder searchSourceBuilder, SearchQuery searchQuery) {
		return executeSearch(prepareSearch(searchSourceBuilder, searchQuery));
	}

	private SearchResult executeSearch(Query query, SearchSourceBuilder request) {
		return executeSearch(prepareQuery(request, query));
	}

	private SearchResult executeSearch(Search search) {
		return new ExtendedSearchResult(execute(search));
	}

	private MultiSearchResult executeMultiSearch(MultiSearch search) {
		return execute(search);
	}

	private Index prepareIndex(IndexQuery query) {
		try {
			String indexName = !hasText(query.getIndexName()) ? retrieveIndexNameFromPersistentEntity(query.getObject().getClass())[0] : query.getIndexName();
			String type = !hasText(query.getType()) ? retrieveTypeFromPersistentEntity(query.getObject().getClass())[0]
					: query.getType();

			Index.Builder indexBuilder;

			if (query.getObject() != null) {
				String entityId = null;
				if (isDocument(query.getObject().getClass())) {
					entityId = getPersistentEntityId(query.getObject());
				}

				indexBuilder = new Index.Builder(resultsMapper.getEntityMapper().mapToString(query.getObject()));

				// If we have a query id and a document id, do not ask ES to generate one.
				if (entityId != null) {
					indexBuilder.index(indexName).type(type).id(entityId);
				} else {
					indexBuilder.index(indexName).type(type);
				}
			} else if (query.getSource() != null) {
				indexBuilder = new Index.Builder(query.getSource()).index(indexName).type(type);
			} else {
				throw new ElasticsearchException("object or source is null, failed to index the document [id: " + query.getId() + "]");
			}

			if (query.getVersion() != null) {
				indexBuilder.setParameter(Parameters.VERSION, query.getVersion());
				indexBuilder.setParameter(Parameters.VERSION_TYPE, EXTERNAL.name().toLowerCase());
			}

			if (query.getId() != null) {
				indexBuilder.id(query.getId());
			}

			if (query.getParentId() != null) {
				indexBuilder.setParameter(Parameters.PARENT, query.getParentId());
			}

			return indexBuilder.build();
		} catch (IOException e) {
			throw new ElasticsearchException("failed to index the document [id: " + query.getId() + "]", e);
		}
	}

	private Update prepareUpdate(UpdateQuery query) {
		String indexName = hasText(query.getIndexName()) ? query.getIndexName() : getPersistentEntityFor(query.getClazz()).getIndexName();
		String type = hasText(query.getType()) ? query.getType() : getPersistentEntityFor(query.getClazz()).getIndexType();
		Assert.notNull(indexName, "No index defined for Query");
		Assert.notNull(type, "No type define for Query");
		Assert.notNull(query.getId(), "No Id define for Query");
		Assert.notNull(query.getUpdateRequest(), "No IndexRequest define for Query");

		Map<String, Object> payLoadMap = new HashMap<>();

		if (query.getUpdateRequest().script() == null) {

			// doc
			if (query.DoUpsert()) {
				payLoadMap.put("doc_as_upsert", Boolean.TRUE);
				payLoadMap.put("doc", query.getUpdateRequest().doc().sourceAsMap());
			} else {
				payLoadMap.put("doc", query.getUpdateRequest().doc().sourceAsMap());
			}
		} else {
			// or script
			/*
			.setScript(query.getUpdateRequest().script(), query.getUpdateRequest().scriptType())
			.setScriptParams(query.getUpdateRequest().scriptParams())
			.setScriptLang(query.getUpdateRequest().scriptLang());
			*/
		}

		try {
			String payload = resultsMapper.getEntityMapper().mapToString(payLoadMap);

			Update.Builder updateBuilder = new Update.Builder(payload).index(indexName).type(type).id(query.getId());

			return updateBuilder.build();
		} catch (IOException e) {
			throw new ElasticsearchException("failed to index the document [id: " + query.getId() + "]", e);
		}
	}

	private <T> Map getDefaultSettings(ElasticsearchPersistentEntity<T> persistentEntity) {

		if (persistentEntity.isUseServerConfiguration())
			return new HashMap();

		return new MapBuilder<String, String>().put("index.number_of_shards", String.valueOf(persistentEntity.getShards()))
				.put("index.number_of_replicas", String.valueOf(persistentEntity.getReplicas()))
				.put("index.refresh_interval", persistentEntity.getRefreshInterval())
				.put("index.store.type", persistentEntity.getIndexStoreType()).map();
	}

	private <T> boolean createIndexIfNotCreated(Class<T> clazz) {
		return indexExists(getPersistentEntityFor(clazz).getIndexName()) || createIndexWithSettings(clazz);
	}

	private <T> boolean createIndexWithSettings(Class<T> clazz) {
		if (clazz.isAnnotationPresent(Setting.class)) {
			String settingPath = clazz.getAnnotation(Setting.class).settingPath();
			if (hasText(settingPath)) {
				String settings = readFileFromClasspath(settingPath);
				if (hasText(settings)) {
					return createIndex(getPersistentEntityFor(clazz).getIndexName(), settings);
				}
			} else {
				logger.info("settingPath in @Setting has to be defined. Using default instead.");
			}
		}
		return createIndex(getPersistentEntityFor(clazz).getIndexName(), getDefaultSettings(getPersistentEntityFor(clazz)));
	}

	@SuppressWarnings("unchecked")
	private boolean isDocument(Class clazz) {
		return clazz.isAnnotationPresent(Document.class);
	}

	private String getPersistentEntityId(Object entity) {

		ElasticsearchPersistentEntity<?> persistentEntity = getPersistentEntityFor(entity.getClass());
		Object identifier = persistentEntity.getIdentifierAccessor(entity).getIdentifier();

		return Optional.ofNullable(identifier).map(String::valueOf).orElse(null);
	}

	private static String[] toArray(List<String> values) {
		String[] valuesAsArray = new String[values.size()];
		return values.toArray(valuesAsArray);
	}

	private static MoreLikeThisQueryBuilder.Item[] toArray(MoreLikeThisQueryBuilder.Item... values) {
		return values;
	}

	private void setPersistentEntityId(Object entity, String id) {

		ElasticsearchPersistentEntity<?> persistentEntity = getPersistentEntityFor(entity.getClass());
		ElasticsearchPersistentProperty idProperty = persistentEntity.getIdProperty();

		// Only deal with text because ES generated Ids are strings !
		if (idProperty != null) {
			if (idProperty.getType().isAssignableFrom(String.class)) {
				persistentEntity.getPropertyAccessor(entity).setProperty(idProperty, id);
			}
		}
	}

	private void setPersistentEntityIndexAndType(Query query, Class clazz) {
		if (query.getIndices().isEmpty()) {
			query.addIndices(retrieveIndexNameFromPersistentEntity(clazz));
		}
		if (query.getTypes().isEmpty()) {
			query.addTypes(retrieveTypeFromPersistentEntity(clazz));
		}
	}


	private String[] retrieveIndexNameFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return new String[]{getPersistentEntityFor(clazz).getIndexName()};
		}
		return null;
	}

	private String[] retrieveTypeFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return new String[]{getPersistentEntityFor(clazz).getIndexType()};
		}
		return null;
	}

	private List<String> extractIds(SearchResult result) {
		List<String> ids = new ArrayList<>();
		for (SearchResult.Hit<JsonObject, Void> hit : result.getHits(JsonObject.class)) {
			if (hit != null) {
				ids.add(hit.id);
			}
		}
		return ids;
	}
}
