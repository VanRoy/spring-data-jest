/*
 * Copyright 2014-2015 the original author or authors.
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
package com.github.vanroy.springdata.jest;

import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import com.github.vanroy.springdata.jest.aggregation.impl.AggregatedPageImpl;
import com.github.vanroy.springdata.jest.entities.*;
import com.github.vanroy.springdata.jest.exception.JestElasticsearchException;
import com.github.vanroy.springdata.jest.internal.ExtendedSearchResult;
import com.github.vanroy.springdata.jest.internal.MultiDocumentResult;
import com.github.vanroy.springdata.jest.internal.SearchScrollResult;
import com.github.vanroy.springdata.jest.mapper.JestMultiGetResultMapper;
import com.github.vanroy.springdata.jest.mapper.JestResultsMapper;
import com.github.vanroy.springdata.jest.mapper.JestSearchResultMapper;
import com.github.vanroy.springdata.jest.provider.CustomSearchSourceBuilderProvider;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.ElasticsearchException;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.EntityMapper;
import org.springframework.data.elasticsearch.core.ScrolledPage;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.data.util.CloseableIterator;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

import static com.github.vanroy.springdata.jest.utils.IndexBuilder.buildIndex;
import static org.apache.commons.lang.RandomStringUtils.randomNumeric;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * @author Rizwan Idrees
 * @author Mohsin Husen
 * @author Franck Marchand
 * @author Abdul Mohammed
 * @author Kevin Leturc
 * @author Mason Chan
 * @author Julien Roy
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:elasticsearch-jest-template-test.xml")
public class JestElasticsearchTemplateTests {

	private static final String INDEX_NAME = "test-index";
	private static final String INDEX_1_NAME = "test-index-1";
	private static final String INDEX_2_NAME = "test-index-2";
	private static final String INDEX_ALIAS_NAME = "test-index-alias";
	private static final String TYPE_NAME = "test-type";
	private static final String ALIAS_NAME = "test-alias";

	@Autowired
	private JestElasticsearchTemplate elasticsearchTemplate;

	@Autowired
	private JestClient jestClient;

	private static List<IndexQuery> createSampleEntitiesWithMessage(String message, int numberOfEntities) {
		List<IndexQuery> indexQueries = new ArrayList<>();
		for (int i = 0; i < numberOfEntities; i++) {
			String documentId = UUID.randomUUID().toString();
			SampleEntity sampleEntity = new SampleEntity();
			sampleEntity.setId(documentId);
			sampleEntity.setMessage(message);
			sampleEntity.setRate(2);
			sampleEntity.setVersion(System.currentTimeMillis());
			IndexQuery indexQuery = new IndexQuery();
			indexQuery.setId(documentId);
			indexQuery.setObject(sampleEntity);
			indexQueries.add(indexQuery);
		}
		return indexQueries;
	}

	@Before
	public void before() {
		elasticsearchTemplate.deleteIndex(INDEX_ALIAS_NAME);
		elasticsearchTemplate.deleteIndex(SampleEntity.class);
		elasticsearchTemplate.createIndex(SampleEntity.class);
		elasticsearchTemplate.putMapping(SampleEntity.class);
		elasticsearchTemplate.deleteIndex(INDEX_1_NAME);
		elasticsearchTemplate.deleteIndex(INDEX_2_NAME);
		elasticsearchTemplate.deleteIndex(UseServerConfigurationEntity.class);
		elasticsearchTemplate.refresh(SampleEntity.class);
	}

	/*
	DATAES-106
	 */
	@Test
	public void shouldReturnCountForGivenCriteriaQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		// when
		long count = elasticsearchTemplate.count(criteriaQuery, SampleEntity.class);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	@Test
	public void shouldReturnCountForGivenSearchQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
		// when
		long count = elasticsearchTemplate.count(searchQuery, SampleEntity.class);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	@Test
	public void shouldReturnObjectForGivenId() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();
		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		// when
		GetQuery getQuery = new GetQuery();
		getQuery.setId(documentId);
		SampleEntity sampleEntity1 = elasticsearchTemplate.queryForObject(getQuery, SampleEntity.class);
		// then
		assertNotNull("entity can't be null....", sampleEntity1);
		assertEquals(sampleEntity, sampleEntity1);
	}

	@Test
	public void shouldReturnNullForInexistentId() {
		// given
		// when
		GetQuery getQuery = new GetQuery();
		getQuery.setId("1");
		SampleEntity sampleEntity1 = elasticsearchTemplate.queryForObject(getQuery, SampleEntity.class);
		// then
		assertNull("entity must be null....", sampleEntity1);
	}

	@Test
	public void shouldReturnObjectsForGivenIdsUsingMultiGet() {
		// given
		List<IndexQuery> indexQueries;
		// first document
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		// second document
		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2).message("some message")
				.version(System.currentTimeMillis()).build();

		indexQueries = getIndexQueries(Arrays.asList(sampleEntity1, sampleEntity2));

		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);

		// when
		SearchQuery query = new NativeSearchQueryBuilder().withIds(Arrays.asList(documentId, documentId2)).build();
		LinkedList<SampleEntity> sampleEntities = elasticsearchTemplate.multiGet(query, SampleEntity.class);
		// then
		assertThat(sampleEntities.size(), is(equalTo(2)));
		assertEquals(sampleEntities.get(0), sampleEntity1);
		assertEquals(sampleEntities.get(1), sampleEntity2);
	}

	@Test
	public void shouldReturnEmptyListForGivenIdsUsingMultiGet() {
		// when
		SearchQuery query = new NativeSearchQueryBuilder().withIds(Arrays.asList("not_exist_id")).build();
		LinkedList<SampleEntity> sampleEntities = elasticsearchTemplate.multiGet(query, SampleEntity.class);
		// then
		assertThat(sampleEntities.size(), is(equalTo(0)));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldReturnObjectsForGivenIdsUsingMultiGetWithFields() {
		// given
		List<IndexQuery> indexQueries;
		// first document
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId)
				.message("some message")
				.type("type1")
				.version(System.currentTimeMillis()).build();

		// second document
		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2)
				.message("some message")
				.type("type2")
				.version(System.currentTimeMillis()).build();

		indexQueries = getIndexQueries(Arrays.asList(sampleEntity1, sampleEntity2));

		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);

		// when
		SearchQuery query = new NativeSearchQueryBuilder()
				.withIds(Arrays.asList(documentId, documentId2))
				.withFields("message", "type")
				.build();
		LinkedList<SampleEntity> sampleEntities = elasticsearchTemplate.multiGet(query, SampleEntity.class, new JestMultiGetResultMapper() {
			@Override
			public <T> LinkedList<T> mapResults(MultiDocumentResult responses, Class<T> clazz) {
				LinkedList<SampleEntity> list = new LinkedList<>();
				for (MultiDocumentResult.MultiDocumentResultItem response : responses.getItems()) {
					SampleEntity entity = new SampleEntity();
					entity.setId(response.getId());

					//TODO: Need to map Fields
//					entity.setMessage((String) response.getField("message").getValue());
//					entity.setType((String) response.getField("type").getValue());
					list.add(entity);
				}
				return (LinkedList<T>) list;
			}
		});
		// then
		assertThat(sampleEntities.size(), is(equalTo(2)));
	}

	@Test
	public void shouldThrowExceptionForInvalidSearchQuery() {
		// given
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withIndices("inexisting_index").withQuery(matchAllQuery()).build();
		// when
		JestElasticsearchException ex = null;

		try {
			elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		} catch (JestElasticsearchException exception) {
			ex = exception;
		}

		assertThat(ex, notNullValue());
		assertThat(ex, is(instanceOf(JestElasticsearchException.class)));
		assertThat(ex.getResult(), notNullValue());
		assertThat(ex.getMessage(), startsWith("Cannot execute jest action , response code : 404 , error"));
	}

	@Test
	public void shouldReturnPageForGivenSearchQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities, is(notNullValue()));
		assertThat(sampleEntities.getTotalElements(), greaterThanOrEqualTo(1L));
		assertThat(sampleEntities.getContent().get(0), is(sampleEntity));
	}

	@Test
	public void shouldDoBulkIndex() {
		// given
		List<IndexQuery> indexQueries;
		// first document
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		// second document
		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2).message("some message")
				.version(System.currentTimeMillis()).build();

		indexQueries = getIndexQueries(Arrays.asList(sampleEntity1, sampleEntity2));

		// when
		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		assertThat(sampleEntities.getTotalElements(), is(equalTo(2L)));
	}

	@Test
	public void shouldDoBulkUpdate() {
		//given
		String documentId = randomNumeric(5);
		String messageBeforeUpdate = "some test message";
		String messageAfterUpdate = "test message";

		SampleEntity sampleEntity = SampleEntity.builder().id(documentId)
				.message(messageBeforeUpdate)
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		IndexRequest indexRequest = new IndexRequest();
		indexRequest.source("message", messageAfterUpdate);
		UpdateQuery updateQuery = new UpdateQueryBuilder().withId(documentId)
				.withClass(SampleEntity.class).withIndexRequest(indexRequest).build();

		List<UpdateQuery> queries = new ArrayList<>();
		queries.add(updateQuery);

		// when
		elasticsearchTemplate.bulkUpdate(queries);
		//then
		GetQuery getQuery = new GetQuery();
		getQuery.setId(documentId);
		SampleEntity indexedEntity = elasticsearchTemplate.queryForObject(getQuery, SampleEntity.class);
		assertThat(indexedEntity.getMessage(), is(messageAfterUpdate));
	}

	@Test
	public void shouldDeleteDocumentForGivenId() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		// when
		elasticsearchTemplate.delete(INDEX_NAME, TYPE_NAME, documentId);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(termQuery("id", documentId)).build();
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		assertThat(sampleEntities.getTotalElements(), equalTo(0L));
	}

	@Test
	public void shouldDeleteEntityForGivenId() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		// when
		elasticsearchTemplate.delete(SampleEntity.class, documentId);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(termQuery("id", documentId)).build();
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		assertThat(sampleEntities.getTotalElements(), equalTo(0L));
	}

	@Test
	public void shouldDeleteDocumentForGivenQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		// when
		DeleteQuery deleteQuery = new DeleteQuery();
		deleteQuery.setQuery(termQuery("id", documentId));
		elasticsearchTemplate.delete(deleteQuery, SampleEntity.class);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(termQuery("id", documentId)).build();
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		assertThat(sampleEntities.getTotalElements(), equalTo(0L));
	}

	@Test
	public void shouldFilterSearchResultsForGivenFilter() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery())
				.withFilter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("id", documentId))).build();
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities.getTotalElements(), equalTo(1L));
	}

	@Test
	public void shouldSortResultsGivenSortCriteria() {
		// given
		List<IndexQuery> indexQueries = new ArrayList<>();
		// first document
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId)
				.message("abc")
				.rate(10)
				.version(System.currentTimeMillis()).build();

		// second document
		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2)
				.message("xyz")
				.rate(5)
				.version(System.currentTimeMillis()).build();

		// third document
		String documentId3 = randomNumeric(5);
		SampleEntity sampleEntity3 = SampleEntity.builder().id(documentId3)
				.message("xyz")
				.rate(15)
				.version(System.currentTimeMillis()).build();

		indexQueries = getIndexQueries(Arrays.asList(sampleEntity1, sampleEntity2, sampleEntity3));

		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery())
				.withSort(new FieldSortBuilder("rate").order(SortOrder.ASC)).build();
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities.getTotalElements(), equalTo(3L));
		assertThat(sampleEntities.getContent().get(0).getRate(), is(sampleEntity2.getRate()));
	}

	@Test
	public void shouldSortResultsGivenMultipleSortCriteria() {
		// given
		List<IndexQuery> indexQueries;
		// first document
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId)
				.message("abc")
				.rate(10)
				.version(System.currentTimeMillis()).build();

		// second document
		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2)
				.message("xyz")
				.rate(5)
				.version(System.currentTimeMillis()).build();

		// third document
		String documentId3 = randomNumeric(5);
		SampleEntity sampleEntity3 = SampleEntity.builder().id(documentId3)
				.message("xyz")
				.rate(15)
				.version(System.currentTimeMillis()).build();

		indexQueries = getIndexQueries(Arrays.asList(sampleEntity1, sampleEntity2, sampleEntity3));

		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery())
				.withSort(new FieldSortBuilder("rate").order(SortOrder.ASC))
				.withSort(new FieldSortBuilder("message").order(SortOrder.ASC)).build();
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities.getTotalElements(), equalTo(3L));
		assertThat(sampleEntities.getContent().get(0).getRate(), is(sampleEntity2.getRate()));
		assertThat(sampleEntities.getContent().get(1).getMessage(), is(sampleEntity1.getMessage()));
	}

	@Test
	public void shouldExecuteStringQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		StringQuery stringQuery = new StringQuery(matchAllQuery().toString());
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(stringQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities.getTotalElements(), equalTo(1L));
		assertThat(sampleEntities.getContent().get(0), equalTo(sampleEntity));
	}

	@Test
	@Ignore("Find how to activate plugins")
	public void shouldUseScriptedFields() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.setId(documentId);
		sampleEntity.setRate(2);
		sampleEntity.setMessage("some message");
		sampleEntity.setVersion(System.currentTimeMillis());

		IndexQuery indexQuery = new IndexQuery();
		indexQuery.setId(documentId);
		indexQuery.setObject(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		Map<String, Object> params = new HashMap<>();
		params.put("factor", 2);
		// when
		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.withScriptField(new ScriptField("scriptedRate",
						new Script(ScriptType.INLINE, "expression", "doc['rate'] * factor", params)))
				.build();
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities.getTotalElements(), equalTo(1L));
		assertThat(sampleEntities.getContent().get(0).getScriptedRate(), equalTo(4.0));
	}

	@Test
	public void shouldReturnPageableResultsGivenStringQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId).message("some message 1")
				.version(System.currentTimeMillis()).build();
		
		documentId = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId).message("some message 2")
				.version(System.currentTimeMillis()).build();

		elasticsearchTemplate.index(getIndexQuery(sampleEntity1));
		elasticsearchTemplate.index(getIndexQuery(sampleEntity2));
		elasticsearchTemplate.refresh(SampleEntity.class);

		StringQuery stringQuery = new StringQuery(matchAllQuery().toString(), PageRequest.of(0, 1));
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(stringQuery, SampleEntity.class);

		// then
		assertEquals(2, sampleEntities.getTotalElements());
		assertEquals(2, sampleEntities.getTotalPages());
		assertTrue(sampleEntities.hasNext());
	}

	@Test
	public void shouldReturnSortedPageableResultsGivenStringQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.setId(documentId);
		sampleEntity.setMessage("some message");
		sampleEntity.setVersion(System.currentTimeMillis());

		IndexQuery indexQuery = new IndexQuery();
		indexQuery.setId(documentId);
		indexQuery.setObject(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		StringQuery stringQuery = new StringQuery(matchAllQuery().toString(), PageRequest.of(0, 10), Sort.by(Sort.Direction.ASC, "message"));
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(stringQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities.getTotalElements(), is(greaterThanOrEqualTo(1L)));
	}

	@Test
	public void shouldReturnObjectMatchingGivenStringQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		StringQuery stringQuery = new StringQuery(termQuery("id", documentId).toString());
		// when
		SampleEntity sampleEntity1 = elasticsearchTemplate.queryForObject(stringQuery, SampleEntity.class);
		// then
		assertThat(sampleEntity1, is(notNullValue()));
		assertThat(sampleEntity1.getId(), is(equalTo(documentId)));
	}

	@Test
	public void shouldCreateIndexGivenEntityClass() {
		// when
		boolean created = elasticsearchTemplate.createIndex(SampleEntity.class);
		final Map setting = elasticsearchTemplate.getSetting(SampleEntity.class);
		// then
		assertThat(created, is(true));
		assertThat(setting.get("index.number_of_shards"), Matchers.is("1"));
		assertThat(setting.get("index.number_of_replicas"), Matchers.is("0"));
	}

	@Test
	public void shouldExecuteGivenCriteriaQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("test message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria("message").contains("test"));

		// when
		SampleEntity sampleEntity1 = elasticsearchTemplate.queryForObject(criteriaQuery, SampleEntity.class);
		// then
		assertThat(sampleEntity1, is(notNullValue()));
	}

	@Test
	public void shouldDeleteGivenCriteriaQuery() throws InterruptedException {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("test message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria("message").contains("test"));

		// when
		elasticsearchTemplate.delete(criteriaQuery, SampleEntity.class);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then
		StringQuery stringQuery = new StringQuery(matchAllQuery().toString());
		List<SampleEntity> sampleEntities = elasticsearchTemplate.queryForList(stringQuery, SampleEntity.class);

		assertThat(sampleEntities.size(), is(0));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldReturnSpecifiedFields() {
		// given
		String documentId = randomNumeric(5);
		String message = "some test message";
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message(message)
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME).withFields("message").build();
		// when
		AggregatedPage<String> page = elasticsearchTemplate.queryForPage(searchQuery, String.class, new JestSearchResultMapper() {
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable) {
				List<String> values = new ArrayList<>();
				for (JsonElement hit : response.getJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray()) {
					values.add(hit.getAsJsonObject().get("_source").getAsJsonObject().get("message").getAsString());
				}
				return new AggregatedPageImpl<T>((List<T>) values);
			}

			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable) {
				return mapResults(response, clazz, pageable);
			}
		});
		// then
		assertThat(page, is(notNullValue()));
		assertThat(page.getTotalElements(), is(equalTo(1L)));
		assertThat(page.getContent().get(0), is(message));
	}

	@Test
	public void shouldReturnFieldsBasedOnSourceFilter() {
		// given
		String documentId = randomNumeric(5);
		String message = "some test message";
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message(message)
				.type("type1")
				.rate(11)
				.available(true)
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		FetchSourceFilterBuilder sourceFilter = new FetchSourceFilterBuilder();
		sourceFilter.withIncludes("message", "available", "rate").withExcludes("r*");

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME).withSourceFilter(sourceFilter.build()).build();
		// when
		Page<SampleEntity> page = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		assertThat(page, is(notNullValue()));
		assertThat(page.getTotalElements(), is(equalTo(1L)));
		assertThat(page.getContent().get(0).getMessage(), is(message));
		assertThat(page.getContent().get(0).getVersion(), is(nullValue()));
		assertThat(page.getContent().get(0).getType(), is(nullValue()));
		assertThat(page.getContent().get(0).getRate(), is(equalTo(0)));
		assertThat(page.getContent().get(0).isAvailable(), is(equalTo(true)));
	}

	@Test
	public void shouldReturnSimilarResultsGivenMoreLikeThisQuery() {
		// given
		String sampleMessage = "So we build a web site or an application and want to add search to it, "
				+ "and then it hits us: getting search working is hard. We want our search solution to be fast,"
				+ " we want a painless setup and a completely free search schema, we want to be able to index data simply using JSON over HTTP, "
				+ "we want our search server to be always available, we want to be able to start with one machine and scale to hundreds, "
				+ "we want real-time search, we want simple multi-tenancy, and we want a solution that is built for the cloud.";

		String documentId1 = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId1).message(sampleMessage)
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);

		String documentId2 = randomNumeric(5);

		elasticsearchTemplate.index(getIndexQuery(SampleEntity.builder().id(documentId2).message(sampleMessage)
				.version(System.currentTimeMillis()).build()));
		elasticsearchTemplate.refresh(SampleEntity.class);

		MoreLikeThisQuery moreLikeThisQuery = new MoreLikeThisQuery();
		moreLikeThisQuery.setId(documentId2);
		moreLikeThisQuery.addFields("message");
		moreLikeThisQuery.setMinDocFreq(1);
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.moreLikeThis(moreLikeThisQuery, SampleEntity.class);

		// then
		assertThat(sampleEntities.getTotalElements(), is(equalTo(1L)));
		assertThat(sampleEntities.getContent(), hasItem(sampleEntity));
	}

	/*
	DATAES-167
	 */
	@Test
	public void shouldReturnResultsWithScanAndScrollForGivenCriteriaQuery() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.addIndices(INDEX_NAME);
		criteriaQuery.addTypes(TYPE_NAME);
		criteriaQuery.setPageable(PageRequest.of(0, 10));

		ScrolledPage<SampleEntity> scroll = (ScrolledPage<SampleEntity>) elasticsearchTemplate.startScroll( 1000, criteriaQuery, SampleEntity.class);
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (scroll.hasContent()) {
			sampleEntities.addAll(scroll.getContent());
			scroll = (ScrolledPage<SampleEntity>) elasticsearchTemplate.continueScroll(scroll.getScrollId() , 1000, SampleEntity.class);
		}
		elasticsearchTemplate.clearScroll(scroll.getScrollId());
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}

	@Test
	public void shouldReturnResultsWithScanAndScrollForGivenSearchQuery() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME).withPageable(PageRequest.of(0, 10)).build();

		ScrolledPage<SampleEntity> scroll = (ScrolledPage<SampleEntity>) elasticsearchTemplate.startScroll(1000, searchQuery, SampleEntity.class);
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (scroll.hasContent()) {
			sampleEntities.addAll(scroll.getContent());
			scroll = (ScrolledPage<SampleEntity>) elasticsearchTemplate.continueScroll(scroll.getScrollId() , 1000, SampleEntity.class);
		}
		elasticsearchTemplate.clearScroll(scroll.getScrollId());
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}

	final JestResultsMapper searchResultMapper = new JestResultsMapper() {

		@Override
		public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable) {
			String scrollId = ((ExtendedSearchResult) response).getScrollId();
			List<T> result = new ArrayList<>();
			for (SearchResult.Hit<T, Void> searchHit : response.getHits(clazz)) {
				if (response.getHits(clazz).size() <= 0) {
					return new AggregatedPageImpl<>(Collections.emptyList(), scrollId);
				}
				result.add(searchHit.source);
			}

			if (result.size() > 0) {
				return new AggregatedPageImpl<>(result, scrollId);
			}
			return new AggregatedPageImpl<>(Collections.emptyList(), scrollId);
		}

		@Override
		public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable) {
			return mapResults(response, clazz, pageable);
		}

		@Override
		public EntityMapper getEntityMapper() {
			return null;
		}

		@Override
		public <T> Page<T> mapResults(SearchScrollResult response, Class<T> clazz) {
			String scrollId = response.getScrollId();
			List<T> result = new ArrayList<>();
			for (SearchScrollResult.Hit<T, Void> searchHit : response.getHits(clazz)) {
				if (response.getHits(clazz).size() <= 0) {
					return new AggregatedPageImpl<>(Collections.emptyList(), scrollId);
				}
				result.add(searchHit.source);
			}

			if (result.size() > 0) {
				return new AggregatedPageImpl<>(result, scrollId);
			}
			return new AggregatedPageImpl<>(Collections.emptyList(), scrollId);
		}

		@Override
		public <T> LinkedList<T> mapResults(MultiDocumentResult response, Class<T> clazz) {
			return null;
		}

		@Override
		public <T> T mapResult(DocumentResult response, Class<T> clazz) {
			return null;
		}
	};

	/*
	DATAES-167
	*/
	@Test
	public void shouldReturnResultsWithScanAndScrollForSpecifiedFieldsForCriteriaQuery() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.addIndices(INDEX_NAME);
		criteriaQuery.addTypes(TYPE_NAME);
		criteriaQuery.addFields("message");
		criteriaQuery.setPageable(PageRequest.of(0, 10));

		Page<SampleEntity> scroll = elasticsearchTemplate.startScroll(1000, criteriaQuery, SampleEntity.class, searchResultMapper);
		String scrollId = ((ScrolledPage<?>)scroll).getScrollId();
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (scroll.hasContent()) {
			sampleEntities.addAll(scroll.getContent());
			scrollId = ((ScrolledPage<?>)scroll).getScrollId();
			scroll =  elasticsearchTemplate.continueScroll(scrollId , 1000, SampleEntity.class, searchResultMapper);
		}
		elasticsearchTemplate. clearScroll(scrollId);
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}

	/*
	DATAES-84
	*/
	@Test
	public void shouldReturnResultsWithScanAndScrollForSpecifiedFieldsForSearchCriteria() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery())
				.withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME)
				.withFields("message")
				.withQuery(matchAllQuery())
				.withPageable(PageRequest.of(0, 10))
				.build();

		Page<SampleEntity> scroll = elasticsearchTemplate.startScroll(1000, searchQuery, SampleEntity.class, searchResultMapper);
		String scrollId = ((ScrolledPage) scroll).getScrollId();
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (scroll.hasContent()) {
			sampleEntities.addAll(scroll.getContent());
			scrollId = ((ScrolledPage) scroll).getScrollId();
			scroll = elasticsearchTemplate.continueScroll(scrollId, 1000, SampleEntity.class, searchResultMapper);
		}
		elasticsearchTemplate.clearScroll(scrollId);
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}


	/*
	DATAES-167
	 */
	@Test
	public void shouldReturnResultsForScanAndScrollWithCustomResultMapperForGivenCriteriaQuery() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.addIndices(INDEX_NAME);
		criteriaQuery.addTypes(TYPE_NAME);
		criteriaQuery.setPageable(PageRequest.of(0, 10));

		Page<SampleEntity> scroll = elasticsearchTemplate.startScroll(1000, criteriaQuery, SampleEntity.class, searchResultMapper);
		String scrollId = ((ScrolledPage) scroll).getScrollId();
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (scroll.hasContent()) {
			sampleEntities.addAll(scroll.getContent());
			scrollId = ((ScrolledPage) scroll).getScrollId();
			scroll = elasticsearchTemplate.continueScroll(scrollId, 1000, SampleEntity.class, searchResultMapper);
		}
		elasticsearchTemplate.clearScroll(scrollId);
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}

	@Test
	public void shouldReturnResultsForScanAndScrollWithCustomResultMapperForGivenSearchQuery() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME).withPageable(PageRequest.of(0, 10)).build();

		Page<SampleEntity> scroll = elasticsearchTemplate.startScroll(1000, searchQuery, SampleEntity.class,searchResultMapper);
		String scrollId = ((ScrolledPage) scroll).getScrollId();
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (scroll.hasContent()) {
			sampleEntities.addAll(scroll.getContent());
			scrollId = ((ScrolledPage) scroll).getScrollId();
			scroll = elasticsearchTemplate.continueScroll(scrollId, 1000, SampleEntity.class, searchResultMapper);
		}
		elasticsearchTemplate.clearScroll(scrollId);
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}

	/*
	DATAES-217
	 */
	@Test
	public void shouldReturnResultsWithScanAndScrollForGivenCriteriaQueryAndClass() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.setPageable(PageRequest.of(0, 10));

		Page<SampleEntity> scroll = elasticsearchTemplate.startScroll(1000, criteriaQuery, SampleEntity.class);
		String scrollId = ((ScrolledPage) scroll).getScrollId();
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (scroll.hasContent()) {
			sampleEntities.addAll(scroll.getContent());
			scrollId = ((ScrolledPage) scroll).getScrollId();
			scroll = elasticsearchTemplate.continueScroll(scrollId, 1000, SampleEntity.class);
		}
		elasticsearchTemplate.clearScroll(scrollId);
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}

	/*
	DATAES-217
	 */
	@Test
	public void shouldReturnResultsWithScanAndScrollForGivenSearchQueryAndClass() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery())
				.withPageable(PageRequest.of(0, 10)).build();

		Page<SampleEntity> scroll = elasticsearchTemplate.startScroll(1000, searchQuery, SampleEntity.class);
		String scrollId = ((ScrolledPage) scroll).getScrollId();
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (scroll.hasContent()) {
			sampleEntities.addAll(scroll.getContent());
			scrollId = ((ScrolledPage) scroll).getScrollId();
			scroll = elasticsearchTemplate.continueScroll(scrollId, 1000, SampleEntity.class);
		}
		elasticsearchTemplate.clearScroll(scrollId);
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}

	/*
	DATAES-167
	 */
	@Test
	public void shouldReturnResultsWithStreamForGivenCriteriaQuery() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then

		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.addIndices(INDEX_NAME);
		criteriaQuery.addTypes(TYPE_NAME);
		criteriaQuery.setPageable(PageRequest.of(0, 10));

		CloseableIterator<SampleEntity> stream = elasticsearchTemplate.stream(criteriaQuery, SampleEntity.class);
		List<SampleEntity> sampleEntities = new ArrayList<>();
		while (stream.hasNext()) {
			sampleEntities.add(stream.next());
		}
		assertThat(sampleEntities.size(), is(equalTo(30)));
	}

	@Test
	public void shouldReturnListForGivenCriteria() {
		// given
		List<IndexQuery> indexQueries;
		// first document
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId)
				.message("test message")
				.version(System.currentTimeMillis()).build();

		// second document
		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2)
				.message("test test")
				.rate(5)
				.version(System.currentTimeMillis()).build();

		// third document
		String documentId3 = randomNumeric(5);
		SampleEntity sampleEntity3 = SampleEntity.builder().id(documentId3)
				.message("some message")
				.rate(15)
				.version(System.currentTimeMillis()).build();

		indexQueries = getIndexQueries(Arrays.asList(sampleEntity1, sampleEntity2, sampleEntity3));

		// when
		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// when
		CriteriaQuery singleCriteriaQuery = new CriteriaQuery(new Criteria("message").contains("test"));
		CriteriaQuery multipleCriteriaQuery = new CriteriaQuery(new Criteria("message").contains("some").and("message")
				.contains("message"));
		List<SampleEntity> sampleEntitiesForSingleCriteria = elasticsearchTemplate.queryForList(singleCriteriaQuery,
				SampleEntity.class);
		List<SampleEntity> sampleEntitiesForAndCriteria = elasticsearchTemplate.queryForList(multipleCriteriaQuery,
				SampleEntity.class);
		// then
		assertThat(sampleEntitiesForSingleCriteria.size(), is(2));
		assertThat(sampleEntitiesForAndCriteria.size(), is(1));
	}

	@Test
	public void shouldReturnListForGivenStringQuery() {
		// given
		// first document
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId)
				.message("test message")
				.version(System.currentTimeMillis()).build();

		// second document
		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2)
				.message("test test")
				.rate(5)
				.version(System.currentTimeMillis()).build();

		// third document
		String documentId3 = randomNumeric(5);
		SampleEntity sampleEntity3 = SampleEntity.builder().id(documentId3)
				.message("some message")
				.rate(15)
				.version(System.currentTimeMillis()).build();

		List<IndexQuery> indexQueries = getIndexQueries(Arrays.asList(sampleEntity1, sampleEntity2, sampleEntity3));

		// when
		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// when
		StringQuery stringQuery = new StringQuery(matchAllQuery().toString());
		List<SampleEntity> sampleEntities = elasticsearchTemplate.queryForList(stringQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities.size(), is(3));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldGetMappingForGivenEntity() throws Exception {
		// given
		Class<SampleMappingEntity> entity = SampleMappingEntity.class;
		elasticsearchTemplate.createIndex(entity);
		elasticsearchTemplate.putMapping(entity);
		// when
		Map<String, Map<String, Map<String, String>>> mapping = elasticsearchTemplate.getMapping(entity);
		// then
		assertThat(mapping.containsKey("properties"), is(true));
		assertThat(mapping.get("properties").containsKey("message"), is(true));
		assertThat(mapping.get("properties").get("message").get("type"), is("text"));
		assertThat(mapping.get("properties").get("message").get("index"), is(false));
		assertThat(mapping.get("properties").get("message").get("store"), is(true));
	}

	@Test
	public void shouldPutMappingForGivenEntity() throws Exception {
		// given
		Class<SampleMappingEntity> entity = SampleMappingEntity.class;
		elasticsearchTemplate.createIndex(entity);
		// when
		assertThat(elasticsearchTemplate.putMapping(entity), is(true));
	}

	@Test
	public void shouldPutXContentBuilderMappingForGivenEntity() throws Exception {
		// given
		elasticsearchTemplate.createIndex("test-mapping");

		XContentBuilder xContentBuilder = JsonXContent.contentBuilder()
			.startObject()
			.field("properties")
				.startObject()
				.field("message")
					.startObject()
						.field("type", "text")
						.field("index", false)
						.field("store", true)
						.field("analyzer", "standard")
					.endObject()
				.endObject()
			.endObject();

		// when
		assertThat(elasticsearchTemplate.putMapping("test-mapping", "mapping", xContentBuilder), is(true));
	}

	@Test
	public void shouldDeleteIndexForGivenEntity() {
		// given
		Class<SampleEntity> clazz = SampleEntity.class;
		// when
		elasticsearchTemplate.deleteIndex(clazz);
		// then
		assertThat(elasticsearchTemplate.indexExists(clazz), is(false));
	}

	@Test
	public void shouldDoPartialUpdateForExistingDocument() {
		//given
		String documentId = randomNumeric(5);
		String messageBeforeUpdate = "some test message";
		String messageAfterUpdate = "test message";

		SampleEntity sampleEntity = SampleEntity.builder().id(documentId)
				.message(messageBeforeUpdate)
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		IndexRequest indexRequest = new IndexRequest();
		indexRequest.source("message", messageAfterUpdate);
		UpdateQuery updateQuery = new UpdateQueryBuilder().withId(documentId)
				.withClass(SampleEntity.class).withIndexRequest(indexRequest).build();
		// when
		elasticsearchTemplate.update(updateQuery);
		//then
		GetQuery getQuery = new GetQuery();
		getQuery.setId(documentId);
		SampleEntity indexedEntity = elasticsearchTemplate.queryForObject(getQuery, SampleEntity.class);
		assertThat(indexedEntity.getMessage(), is(messageAfterUpdate));
	}

	@Test(expected = DocumentMissingException.class)
	@Ignore("Not yet implemented with Jest")
	public void shouldThrowExceptionIfDocumentDoesNotExistWhileDoingPartialUpdate() {
		// when
		IndexRequest indexRequest = new IndexRequest();
		UpdateQuery updateQuery = new UpdateQueryBuilder().withId(randomNumeric(5))
				.withClass(SampleEntity.class).withIndexRequest(indexRequest).build();
		elasticsearchTemplate.update(updateQuery);
	}

	@Test
	public void shouldDoUpsertIfDocumentDoesNotExist() {
		//given
		String documentId = randomNumeric(5);
		String message = "test message";
		IndexRequest indexRequest = new IndexRequest();
		indexRequest.source("message", message);
		UpdateQuery updateQuery = new UpdateQueryBuilder().withId(documentId)
				.withDoUpsert(true).withClass(SampleEntity.class)
				.withIndexRequest(indexRequest).build();
		//when
		elasticsearchTemplate.update(updateQuery);
		//then
		GetQuery getQuery = new GetQuery();
		getQuery.setId(documentId);
		SampleEntity indexedEntity = elasticsearchTemplate.queryForObject(getQuery, SampleEntity.class);
		assertThat(indexedEntity.getMessage(), is(message));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldReturnHighlightedFieldsForGivenQueryAndFields() {

		//given
		String documentId = randomNumeric(5);
		String actualMessage = "some test message";
		String highlightedMessage = "some <em>test</em> message";

		SampleEntity sampleEntity = SampleEntity.builder().id(documentId)
				.message(actualMessage)
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(termQuery("message", "test"))
				.withHighlightFields(new HighlightBuilder.Field("message"))
				.build();

		AggregatedPage<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class, new JestSearchResultMapper() {
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable) {
				List<SampleEntity> chunk = new ArrayList<>();
				for (SearchResult.Hit<JsonObject, Void> searchHit : response.getHits(JsonObject.class)) {
					if (response.getHits(JsonObject.class).size() <= 0) {
						return null;
					}
					SampleEntity user = new SampleEntity();
					user.setId(searchHit.source.get(JestResult.ES_METADATA_ID).getAsString());
					user.setMessage(searchHit.source.get("message").getAsString());
					user.setHighlightedMessage(searchHit.highlight.get("message").get(0));
					chunk.add(user);
				}
				if (chunk.size() > 0) {
					return new AggregatedPageImpl<T>((List<T>) chunk);
				}
				return null;
			}
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable) {
				return mapResults(response, clazz, pageable);
			}
		});

		assertThat(sampleEntities.getContent().get(0).getHighlightedMessage(), is(highlightedMessage));
	}

	@Test
	public void shouldDeleteDocumentBySpecifiedTypeUsingDeleteQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId)
				.message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// when
		DeleteQuery deleteQuery = new DeleteQuery();
		deleteQuery.setQuery(termQuery("id", documentId));
		deleteQuery.setIndex(INDEX_NAME);
		deleteQuery.setType(TYPE_NAME);
		elasticsearchTemplate.delete(deleteQuery);
		elasticsearchTemplate.refresh(INDEX_NAME);
		// then
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(termQuery("id", documentId)).build();
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		assertThat(sampleEntities.getTotalElements(), equalTo(0L));
	}

	@Test
	public void shouldIndexNotDocumentEntity() {

		// given
		BasicEntity entity = new BasicEntity(randomNumeric(1), "aFirstName");

		IndexQuery indexQuery = new IndexQuery();
		indexQuery.setObject(entity);
		indexQuery.setIndexName(INDEX_NAME);
		indexQuery.setType(TYPE_NAME);
		// when
		String id = elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);

		GetQuery query = new GetQuery();
		query.setId(id);
		AnnotatedBasicEntity indexedEntity = elasticsearchTemplate.queryForObject(query, AnnotatedBasicEntity.class);

		assertThat(indexedEntity, is(notNullValue()));
		assertThat(indexedEntity.getId(), equalTo(id));
		assertThat(indexedEntity.getFirstName(), equalTo(entity.getFirstName()));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldIndexDocumentForSpecifiedSource() {

		// given
		String documentSource = "{\"id\":\"2333343434\",\"type\":null,\"message\":\"some message\",\"rate\":0,\"available\":false,\"highlightedMessage\":null,\"version\":1385208779482}";
		IndexQuery indexQuery = new IndexQuery();
		indexQuery.setId("2333343434");
		indexQuery.setSource(documentSource);
		indexQuery.setIndexName(INDEX_NAME);
		indexQuery.setType(TYPE_NAME);
		// when
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(termQuery("id", indexQuery.getId()))
				.withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME)
				.build();
		// then
		AggregatedPage<SampleEntity> page = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class, new JestSearchResultMapper() {
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable) {
				List<SampleEntity> values = new ArrayList<SampleEntity>();
				for (SearchResult.Hit<JsonObject, Void> searchHit : response.getHits(JsonObject.class)) {
					SampleEntity sampleEntity = new SampleEntity();
					sampleEntity.setId(searchHit.source.get(JestResult.ES_METADATA_ID).getAsString());
					sampleEntity.setMessage(searchHit.source.get("message").getAsString());
					values.add(sampleEntity);
				}
				return new AggregatedPageImpl<T>((List<T>) values);
			}
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable) {
				return mapResults(response, clazz, pageable);
			}
		});
		assertThat(page, is(notNullValue()));
		assertThat(page.getContent().size(), is(1));
		assertThat(page.getContent().get(0).getId(), is(indexQuery.getId()));
	}

	@Test(expected = ElasticsearchException.class)
	public void shouldThrowElasticsearchExceptionWhenNoDocumentSpecified() {
		// given
		IndexQuery indexQuery = new IndexQuery();
		indexQuery.setId("2333343434");
		indexQuery.setIndexName(INDEX_NAME);
		indexQuery.setType(TYPE_NAME);

		//when
		elasticsearchTemplate.index(indexQuery);
	}

	@Test
	public void shouldReturnIds() {
		//given
		List<IndexQuery> entities = createSampleEntitiesWithMessage("Test message", 30);
		// when
		elasticsearchTemplate.bulkIndex(entities);
		elasticsearchTemplate.refresh(SampleEntity.class);
		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(termQuery("message", "message"))
				.withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME)
				.withPageable(PageRequest.of(0, 100))
				.build();
		// then
		List<String> ids = elasticsearchTemplate.queryForIds(searchQuery);
		assertThat(ids, is(notNullValue()));
		assertThat(ids.size(), is(30));
	}

	@Test
	public void shouldReturnDocumentAboveMinimalScoreGivenQuery() {
		// given
		List<IndexQuery> indexQueries = new ArrayList<>();

		indexQueries.add(buildIndex(SampleEntity.builder().id("1").message("ab").build()));
		indexQueries.add(buildIndex(SampleEntity.builder().id("2").message("bc").build()));
		indexQueries.add(buildIndex(SampleEntity.builder().id("3").message("ac").build()));

		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);

		// when
		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(boolQuery()
					.must(wildcardQuery("message", "*a*"))
					.should(wildcardQuery("message", "*b*"))
				)
				.withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME)
				.withMinScore(2.0F)
				.build();

		Page<SampleEntity> page = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		assertThat(page.getTotalElements(), is(1L));
		assertThat(page.getContent().get(0).getMessage(), is("ab"));
	}


	@Test
	public void shouldDoIndexWithoutId() {
		// given
		// document
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.setMessage("some message");
		sampleEntity.setVersion(System.currentTimeMillis());

		IndexQuery indexQuery = new IndexQuery();
		indexQuery.setObject(sampleEntity);
		// when
		String documentId = elasticsearchTemplate.index(indexQuery);
		// then
		assertThat(sampleEntity.getId(), is(equalTo(documentId)));

		GetQuery getQuery = new GetQuery();
		getQuery.setId(documentId);
		SampleEntity result = elasticsearchTemplate.queryForObject(getQuery, SampleEntity.class);
		assertThat(result.getId(), is(equalTo(documentId)));
	}

	@Test
	public void shouldDoBulkIndexWithoutId() {
		// given
		List<IndexQuery> indexQueries = new ArrayList<>();
		// first document
		SampleEntity sampleEntity1 = new SampleEntity();
		sampleEntity1.setMessage("some message");
		sampleEntity1.setVersion(System.currentTimeMillis());

		IndexQuery indexQuery1 = new IndexQuery();
		indexQuery1.setObject(sampleEntity1);
		indexQueries.add(indexQuery1);

		// second document
		SampleEntity sampleEntity2 = new SampleEntity();
		sampleEntity2.setMessage("some message");
		sampleEntity2.setVersion(System.currentTimeMillis());

		IndexQuery indexQuery2 = new IndexQuery();
		indexQuery2.setObject(sampleEntity2);
		indexQueries.add(indexQuery2);
		// when
		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);
		// then
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		assertThat(sampleEntities.getTotalElements(), is(equalTo(2L)));

		assertThat(sampleEntities.getContent().get(0).getId(), is(notNullValue()));
		assertThat(sampleEntities.getContent().get(1).getId(), is(notNullValue()));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldIndexMapWithIndexNameAndTypeAtRuntime() {
		//given
		Map<String, Object> person1 = new HashMap<>();
		person1.put("userId", "1");
		person1.put("email", "smhdiu@gmail.com");
		person1.put("title", "Mr");
		person1.put("firstName", "Mohsin");
		person1.put("lastName", "Husen");

		Map<String, Object> person2 = new HashMap<>();
		person2.put("userId", "2");
		person2.put("email", "akonczak@gmail.com");
		person2.put("title", "Mr");
		person2.put("firstName", "Artur");
		person2.put("lastName", "Konczak");

		IndexQuery indexQuery1 = new IndexQuery();
		indexQuery1.setId("1");
		indexQuery1.setObject(person1);
		indexQuery1.setIndexName(INDEX_NAME);
		indexQuery1.setType(TYPE_NAME);

		IndexQuery indexQuery2 = new IndexQuery();
		indexQuery2.setId("2");
		indexQuery2.setObject(person2);
		indexQuery2.setIndexName(INDEX_NAME);
		indexQuery2.setType(TYPE_NAME);

		List<IndexQuery> indexQueries = new ArrayList<>();
		indexQueries.add(indexQuery1);
		indexQueries.add(indexQuery2);

		//when
		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(INDEX_NAME);

		// then
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME).withQuery(matchAllQuery()).build();
		AggregatedPage<Map> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, Map.class, new JestSearchResultMapper() {
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable) {
				List<Map> chunk = new ArrayList<>();
				for (SearchResult.Hit<JsonObject, Void> searchHit : response.getHits(JsonObject.class)) {
					if (response.getHits(JsonObject.class).size() <= 0) {
						return null;
					}
					Map<String, Object> person = new HashMap<>();
					person.put("userId", searchHit.source.get("userId").getAsString());
					person.put("email", searchHit.source.get("email").getAsString());
					person.put("title", searchHit.source.get("title").getAsString());
					person.put("firstName", searchHit.source.get("firstName").getAsString());
					person.put("lastName", searchHit.source.get("lastName").getAsString());
					chunk.add(person);
				}
				if (chunk.size() > 0) {
					return new AggregatedPageImpl<T>((List<T>) chunk);
				}
				return null;
			}
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable) {
				return mapResults(response, clazz, pageable);
			}
		});
		assertThat(sampleEntities.getTotalElements(), is(equalTo(2L)));
		assertThat(sampleEntities.getContent().get(0).get("userId"), is(person1.get("userId")));
		assertThat(sampleEntities.getContent().get(1).get("userId"), is(person2.get("userId")));
	}

	@Test
	public void shouldIndexSampleEntityWithIndexAndTypeAtRuntime() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId)
				.message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = new IndexQueryBuilder().withId(documentId)
				.withIndexName(INDEX_NAME).withType(TYPE_NAME)
				.withObject(sampleEntity).build();

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(INDEX_NAME);

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME).withQuery(matchAllQuery()).build();
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		assertThat(sampleEntities, is(notNullValue()));
		assertThat(sampleEntities.getTotalElements(), greaterThanOrEqualTo(1L));
	}

	@Test
	public void shouldIndexVersionedEntity() {
		// given
		String documentId = randomNumeric(5);
		BasicEntity entity = new BasicEntity(documentId, "test");

		IndexQuery indexQuery = new IndexQueryBuilder().withId(documentId)
				.withIndexName(INDEX_NAME).withType(TYPE_NAME)
				.withVersion(entity.getVersion())
				.withObject(entity).build();

		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(INDEX_NAME);

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withIndices(INDEX_NAME)
				.withTypes(TYPE_NAME).withQuery(matchAllQuery()).build();
		// when
		Page<BasicEntity> entities = elasticsearchTemplate.queryForPage(searchQuery, BasicEntity.class);
		// then
		assertThat(entities, is(notNullValue()));
		assertThat(entities.getTotalElements(), greaterThanOrEqualTo(1L));
		assertThat(entities.getContent().get(0).getFirstName(), equalTo(entity.getFirstName()));
		assertThat(entities.getContent().get(0).getVersion(), equalTo(entity.getVersion()));
	}

	/*
	DATAES-106
	 */
	@Test
	public void shouldReturnCountForGivenCriteriaQueryWithGivenIndexUsingCriteriaQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.addIndices("test-index");
		// when
		long count = elasticsearchTemplate.count(criteriaQuery);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	/*
	DATAES-67
	 */
	@Test
	public void shouldReturnCountForGivenSearchQueryWithGivenIndexUsingSearchQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.withIndices("test-index")
				.build();
		// when
		long count = elasticsearchTemplate.count(searchQuery);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	/*
	DATAES-106
	 */
	@Test
	public void shouldReturnCountForGivenCriteriaQueryWithGivenIndexAndTypeUsingCriteriaQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.addIndices("test-index");
		criteriaQuery.addTypes("test-type");
		// when
		long count = elasticsearchTemplate.count(criteriaQuery);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	/*
	DATAES-67
	 */
	@Test
	public void shouldReturnCountForGivenSearchQueryWithGivenIndexAndTypeUsingSearchQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.withIndices("test-index")
				.withTypes("test-type")
				.build();
		// when
		long count = elasticsearchTemplate.count(searchQuery);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	/*
	DATAES-106
	 */
	@Test
	public void shouldReturnCountForGivenCriteriaQueryWithGivenMultiIndices() {
		// given
		cleanUpIndices();
		String documentId1 = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId1).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery1 = new IndexQueryBuilder().withId(sampleEntity1.getId())
				.withIndexName("test-index-1")
				.withObject(sampleEntity1)
				.build();

		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2).message("some test message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery2 = new IndexQueryBuilder().withId(sampleEntity2.getId())
				.withIndexName("test-index-2")
				.withObject(sampleEntity2)
				.build();

		elasticsearchTemplate.bulkIndex(Arrays.asList(indexQuery1, indexQuery2));
		elasticsearchTemplate.refresh("test-index-1");
		elasticsearchTemplate.refresh("test-index-2");

		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.addIndices("test-index-1", "test-index-2");
		// when
		long count = elasticsearchTemplate.count(criteriaQuery);
		// then
		assertThat(count, is(equalTo(2L)));
	}

	/*
	DATAES-67
	 */
	@Test
	public void shouldReturnCountForGivenSearchQueryWithGivenMultiIndices() {
		// given
		cleanUpIndices();
		String documentId1 = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId1).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery1 = new IndexQueryBuilder().withId(sampleEntity1.getId())
				.withIndexName("test-index-1")
				.withObject(sampleEntity1)
				.build();

		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2).message("some test message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery2 = new IndexQueryBuilder().withId(sampleEntity2.getId())
				.withIndexName("test-index-2")
				.withObject(sampleEntity2)
				.build();

		elasticsearchTemplate.bulkIndex(Arrays.asList(indexQuery1, indexQuery2));
		elasticsearchTemplate.refresh("test-index-1");
		elasticsearchTemplate.refresh("test-index-2");

		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.withIndices("test-index-1", "test-index-2")
				.build();
		// when
		long count = elasticsearchTemplate.count(searchQuery);
		// then
		assertThat(count, is(equalTo(2L)));
	}

	private void cleanUpIndices() {
		elasticsearchTemplate.deleteIndex("test-index-1");
		elasticsearchTemplate.deleteIndex("test-index-2");
		elasticsearchTemplate.createIndex("test-index-1");
		elasticsearchTemplate.createIndex("test-index-2");
		elasticsearchTemplate.refresh("test-index-1");
		elasticsearchTemplate.refresh("test-index-2");
	}

	/*
	DATAES-71
	*/
	@Test
	public void shouldCreatedIndexWithSpecifiedIndexName() {
		// given
		elasticsearchTemplate.deleteIndex("test-index");
		// when
		elasticsearchTemplate.createIndex("test-index");
		// then
		assertThat(elasticsearchTemplate.indexExists("test-index"), is(true));
	}

	/*
	DATAES-72
	*/
	@Test
	public void shouldDeleteIndexForSpecifiedIndexName() {
		// given
		elasticsearchTemplate.createIndex(SampleEntity.class);
		elasticsearchTemplate.refresh(SampleEntity.class);

		// when
		elasticsearchTemplate.deleteIndex("test-index");
		// then
		assertThat(elasticsearchTemplate.indexExists("test-index"), is(false));
	}

	/*
	DATAES-106
	 */
	@Test
	public void shouldReturnCountForGivenCriteriaQueryWithGivenIndexNameForSpecificIndex() {
		// given
		cleanUpIndices();
		String documentId1 = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId1).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery1 = new IndexQueryBuilder().withId(sampleEntity1.getId())
				.withIndexName("test-index-1")
				.withObject(sampleEntity1)
				.build();

		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2).message("some test message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery2 = new IndexQueryBuilder().withId(sampleEntity2.getId())
				.withIndexName("test-index-2")
				.withObject(sampleEntity2)
				.build();

		elasticsearchTemplate.bulkIndex(Arrays.asList(indexQuery1, indexQuery2));
		elasticsearchTemplate.refresh("test-index-1");
		elasticsearchTemplate.refresh("test-index-2");

		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		criteriaQuery.addIndices("test-index-1");
		// when
		long count = elasticsearchTemplate.count(criteriaQuery);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	/*
	DATAES-67
	*/
	@Test
	public void shouldReturnCountForGivenSearchQueryWithGivenIndexNameForSpecificIndex() {
		// given
		cleanUpIndices();
		String documentId1 = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId1).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery1 = new IndexQueryBuilder().withId(sampleEntity1.getId())
				.withIndexName("test-index-1")
				.withObject(sampleEntity1)
				.build();

		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2).message("some test message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery2 = new IndexQueryBuilder().withId(sampleEntity2.getId())
				.withIndexName("test-index-2")
				.withObject(sampleEntity2)
				.build();

		elasticsearchTemplate.bulkIndex(Arrays.asList(indexQuery1, indexQuery2));
		elasticsearchTemplate.refresh("test-index-1");
		elasticsearchTemplate.refresh("test-index-2");

		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.withIndices("test-index-1")
				.build();
		// when
		long count = elasticsearchTemplate.count(searchQuery);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowAnExceptionForGivenCriteriaQueryWhenNoIndexSpecifiedForCountQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());
		// when
		long count = elasticsearchTemplate.count(criteriaQuery);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	/*
	DATAES-67
	*/
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowAnExceptionForGivenSearchQueryWhenNoIndexSpecifiedForCountQuery() {
		// given
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity = SampleEntity.builder().id(documentId).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery = getIndexQuery(sampleEntity);
		elasticsearchTemplate.index(indexQuery);
		elasticsearchTemplate.refresh(SampleEntity.class);
		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.build();
		// when
		long count = elasticsearchTemplate.count(searchQuery);
		// then
		assertThat(count, is(equalTo(1L)));
	}

	/*
	DATAES-71
	*/
	@Test
	public void shouldCreateIndexWithGivenSettings() {
		// given
		String settings = "{\n" +
				"        \"index\": {\n" +
				"            \"number_of_shards\": \"1\",\n" +
				"            \"number_of_replicas\": \"0\",\n" +
				"            \"analysis\": {\n" +
				"                \"analyzer\": {\n" +
				"                    \"emailAnalyzer\": {\n" +
				"                        \"type\": \"custom\",\n" +
				"                        \"tokenizer\": \"uax_url_email\"\n" +
				"                    }\n" +
				"                }\n" +
				"            }\n" +
				"        }\n" +
				"}";

		elasticsearchTemplate.deleteIndex("test-index");
		// when
		elasticsearchTemplate.createIndex("test-index", settings);
		// then
		Map map = elasticsearchTemplate.getSetting("test-index");
		boolean hasAnalyzer = map.containsKey("index.analysis.analyzer.emailAnalyzer.tokenizer");
		String emailAnalyzer = (String) map.get("index.analysis.analyzer.emailAnalyzer.tokenizer");
		assertThat(elasticsearchTemplate.indexExists("test-index"), is(true));
		assertThat(hasAnalyzer, is(true));
		assertThat(emailAnalyzer, is("uax_url_email"));
	}

	/*
	DATAES-71
	*/
	@Test
	public void shouldCreateGivenSettingsForGivenIndex() {
		//given
		//delete , create and apply mapping in before method

		// then
		Map map = elasticsearchTemplate.getSetting(SampleEntity.class);
		assertThat(elasticsearchTemplate.indexExists("test-index"), is(true));
		assertThat(map.containsKey("index.refresh_interval"), is(true));
		assertThat(map.containsKey("index.number_of_replicas"), is(true));
		assertThat(map.containsKey("index.number_of_shards"), is(true));
		assertThat(map.containsKey("index.store.type"), is(true));
		assertThat(map.get("index.refresh_interval"), is("-1"));
		assertThat(map.get("index.number_of_replicas"), is("0"));
		assertThat(map.get("index.number_of_shards"), is("1"));
		assertThat(map.get("index.store.type"), is("fs"));
	}

	/*
	DATAES-88
	*/
	@Test
	public void shouldCreateIndexWithGivenClassAndSettings() {
		//given
		String settings = "{\n" +
				"        \"index\": {\n" +
				"            \"number_of_shards\": \"1\",\n" +
				"            \"number_of_replicas\": \"0\",\n" +
				"            \"analysis\": {\n" +
				"                \"analyzer\": {\n" +
				"                    \"emailAnalyzer\": {\n" +
				"                        \"type\": \"custom\",\n" +
				"                        \"tokenizer\": \"uax_url_email\"\n" +
				"                    }\n" +
				"                }\n" +
				"            }\n" +
				"        }\n" +
				"}";

		elasticsearchTemplate.deleteIndex(SampleEntity.class);
		elasticsearchTemplate.createIndex(SampleEntity.class, settings);
		elasticsearchTemplate.refresh(SampleEntity.class);

		// then
		Map map = elasticsearchTemplate.getSetting(SampleEntity.class);
		assertThat(elasticsearchTemplate.indexExists("test-index"), is(true));
		assertThat(map.containsKey("index.number_of_replicas"), is(true));
		assertThat(map.containsKey("index.number_of_shards"), is(true));
		assertThat(map.get("index.number_of_replicas"), is("0"));
		assertThat(map.get("index.number_of_shards"), is("1"));
	}

	@Test
	public void shouldTestResultsAcrossMultipleIndices() {
		// given
		String documentId1 = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId1).message("some message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery1 = new IndexQueryBuilder().withId(sampleEntity1.getId())
				.withIndexName("test-index-1")
				.withObject(sampleEntity1)
				.build();

		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2).message("some test message")
				.version(System.currentTimeMillis()).build();

		IndexQuery indexQuery2 = new IndexQueryBuilder().withId(sampleEntity2.getId())
				.withIndexName("test-index-2")
				.withObject(sampleEntity2)
				.build();

		elasticsearchTemplate.bulkIndex(Arrays.asList(indexQuery1, indexQuery2));
		elasticsearchTemplate.refresh("test-index-1");
		elasticsearchTemplate.refresh("test-index-2");

		SearchQuery searchQuery = new NativeSearchQueryBuilder()
				.withQuery(matchAllQuery())
				.withIndices("test-index-1", "test-index-2")
				.build();
		// when
		List<SampleEntity> sampleEntities = elasticsearchTemplate.queryForList(searchQuery, SampleEntity.class);

		// then
		assertThat(sampleEntities.size(), is(equalTo(2)));
	}

	/**
	 * This is basically a demonstration to show composing entities out of heterogeneous indexes.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void shouldComposeObjectsReturnedFromHeterogeneousIndexes() {

		// Given

		HetroEntity1 entity1 = new HetroEntity1(randomNumeric(3), "aFirstName");
		HetroEntity2 entity2 = new HetroEntity2(randomNumeric(4), "aLastName");

		IndexQuery idxQuery1 = new IndexQueryBuilder().withIndexName(INDEX_1_NAME).withId(entity1.getId()).withObject(entity1).build();
		IndexQuery idxQuery2 = new IndexQueryBuilder().withIndexName(INDEX_2_NAME).withId(entity2.getId()).withObject(entity2).build();

		elasticsearchTemplate.bulkIndex(Arrays.asList(idxQuery1, idxQuery2));
		elasticsearchTemplate.refresh(INDEX_1_NAME);
		elasticsearchTemplate.refresh(INDEX_2_NAME);

		// When

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).withTypes("hetro").withIndices(INDEX_1_NAME, INDEX_2_NAME).build();
		AggregatedPage<ResultAggregator> page = elasticsearchTemplate.queryForPage(searchQuery, ResultAggregator.class, new JestSearchResultMapper() {
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable) {
				List<ResultAggregator> values = new ArrayList<>();
				for (SearchResult.Hit<JsonObject, Void> searchHit : response.getHits(JsonObject.class)) {
					String id = String.valueOf(searchHit.source.get("id"));
					String firstName = searchHit.source.get("firstName") != null ? searchHit.source.get("firstName").getAsString() : "";
					String lastName = searchHit.source.get("lastName") != null ? searchHit.source.get("lastName").getAsString() : "";
					values.add(new ResultAggregator(id, firstName, lastName));
				}
				return new AggregatedPageImpl<>((List<T>) values);
			}
			@Override
			public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable) {
				return mapResults(response, clazz, pageable);
			}
		});

		assertThat(page.getTotalElements(), is(2L));
	}

	@Test
	public void shouldCreateIndexUsingServerDefaultConfiguration() {
		//given

		//when
		boolean created = elasticsearchTemplate.createIndex(UseServerConfigurationEntity.class);
		//then
		assertThat(created, is(true));
		final Map setting = elasticsearchTemplate.getSetting(UseServerConfigurationEntity.class);
		assertThat(setting.get("index.number_of_shards"), Matchers.is("5"));
		assertThat(setting.get("index.number_of_replicas"), Matchers.is("1"));
	}

	@Test
	public void shouldReadFileFromClasspathRetainingNewlines() {
		// given
		String settingsFile = "/settings/test-settings.yml";

		// when
		String content = JestElasticsearchTemplate.readFileFromClasspath(settingsFile);

		// then
		assertThat(content, is("index:\n" +
				"  number_of_shards: 1\n" +
				"  number_of_replicas: 0\n" +
				"  analysis:\n" +
				"    analyzer:\n" +
				"      emailAnalyzer:\n" +
				"        type: custom\n" +
				"        tokenizer: uax_url_email\n"));
	}

	@Test
	public void shouldGetIndicesFromAlias() {

		elasticsearchTemplate.createIndex(INDEX_ALIAS_NAME);

		AliasQuery aliasQuery = new AliasQuery();
		aliasQuery.setAliasName(ALIAS_NAME);
		aliasQuery.setIndexName(INDEX_ALIAS_NAME);

		elasticsearchTemplate.addAlias(aliasQuery);

		Set<String> indices = elasticsearchTemplate.getIndicesFromAlias(ALIAS_NAME);

		assertThat(indices.size(), is(1));
		assertThat(indices.iterator().next(), is(INDEX_ALIAS_NAME));

	}

	@Test
	public void shouldUseCustomSearchSourceBuilder() {
		// given
		CustomSearchSourceBuilderProvider searchSourceBuilderProviderSpy = Mockito.spy(new CustomSearchSourceBuilderProvider());
		JestElasticsearchTemplate elasticsearchTemplate = new JestElasticsearchTemplate(jestClient, null, null, null, searchSourceBuilderProviderSpy);

		List<IndexQuery> indexQueries = new ArrayList<>();
		// first document
		String documentId = randomNumeric(5);
		SampleEntity sampleEntity1 = SampleEntity.builder().id(documentId)
				.message("abc")
				.rate(10)
				.version(System.currentTimeMillis()).build();

		// second document
		String documentId2 = randomNumeric(5);
		SampleEntity sampleEntity2 = SampleEntity.builder().id(documentId2)
				.message("xyz")
				.rate(5)
				.version(System.currentTimeMillis()).build();

		// third document
		String documentId3 = randomNumeric(5);
		SampleEntity sampleEntity3 = SampleEntity.builder().id(documentId3)
				.message("xyz")
				.rate(15)
				.version(System.currentTimeMillis()).build();

		indexQueries = getIndexQueries(Arrays.asList(sampleEntity1, sampleEntity2, sampleEntity3));

		elasticsearchTemplate.bulkIndex(indexQueries);
		elasticsearchTemplate.refresh(SampleEntity.class);

		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(matchAllQuery())
				.withSort(new FieldSortBuilder("rate").order(SortOrder.ASC)).build();
		// when
		Page<SampleEntity> sampleEntities = elasticsearchTemplate.queryForPage(searchQuery, SampleEntity.class);
		// then
		Mockito.verify(searchSourceBuilderProviderSpy, Mockito.atLeastOnce()).get();
		assertThat(sampleEntities.getTotalElements(), equalTo(3L));
		assertThat(sampleEntities.getContent().get(0).getRate(), is(sampleEntity2.getRate()));
	}

	private IndexQuery getIndexQuery(SampleEntity sampleEntity) {
		return new IndexQueryBuilder().withId(sampleEntity.getId()).withObject(sampleEntity).build();
	}

	private List<IndexQuery> getIndexQueries(List<SampleEntity> sampleEntities) {
		List<IndexQuery> indexQueries = new ArrayList<IndexQuery>();
		for (SampleEntity sampleEntity : sampleEntities) {
			indexQueries.add(new IndexQueryBuilder().withId(sampleEntity.getId()).withObject(sampleEntity).build());
		}
		return indexQueries;
	}

	@Document(indexName = INDEX_2_NAME, replicas = 0, shards = 1)
	class ResultAggregator {

		private final String id;
		private final String firstName;
		private final String lastName;

		ResultAggregator(String id, String firstName, String lastName) {
			this.id = id;
			this.firstName = firstName;
			this.lastName = lastName;
		}
	}
}
