package com.github.vanroy.springdata.jest.mapper;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.github.vanroy.springdata.jest.aggregation.AggregatedPage;
import com.github.vanroy.springdata.jest.aggregation.impl.AggregatedPageImpl;
import com.github.vanroy.springdata.jest.internal.ExtendedSearchResult;
import com.github.vanroy.springdata.jest.internal.MultiDocumentResult;
import com.github.vanroy.springdata.jest.internal.SearchScrollResult;
import com.google.gson.JsonObject;
import io.searchbox.client.JestResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.SearchResult;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.ElasticsearchException;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.DefaultEntityMapper;
import org.springframework.data.elasticsearch.core.EntityMapper;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.springframework.util.StringUtils.hasText;

/**
 * Jest implementation of Spring Data Elasticsearch results mapper.
 *
 * @author Julien Roy
 */
public class DefaultJestResultsMapper implements JestResultsMapper {

	private EntityMapper entityMapper;
	private MappingContext<? extends ElasticsearchPersistentEntity<?>, ElasticsearchPersistentProperty> mappingContext;

	public DefaultJestResultsMapper(EntityMapper entityMapper) {
		this.entityMapper = entityMapper;
	}

	public DefaultJestResultsMapper(MappingContext<? extends ElasticsearchPersistentEntity<?>, ElasticsearchPersistentProperty> mappingContext) {
		this.entityMapper = new DefaultEntityMapper(mappingContext);
		this.mappingContext = mappingContext;
	}

	public DefaultJestResultsMapper(MappingContext<? extends ElasticsearchPersistentEntity<?>, ElasticsearchPersistentProperty> mappingContext, EntityMapper entityMapper) {
		this.entityMapper = entityMapper;
		this.mappingContext = mappingContext;
	}

	public EntityMapper getEntityMapper() {
		return this.entityMapper;
	}

	public <T> T mapResult(DocumentResult response, Class<T> clazz) {
		T result = mapEntity(response.getSourceAsString(), clazz);
		if (result != null) {
			setPersistentEntityId(result, response.getId(), clazz);
		}
		return result;
	}

	public <T> LinkedList<T> mapResults(MultiDocumentResult multiResponse, Class<T> clazz) {

		LinkedList<T> results = new LinkedList<>();

		for (MultiDocumentResult.MultiDocumentResultItem item : multiResponse.getItems()) {
			T result = mapEntity(item.getSource(), clazz);
			setPersistentEntityId(result, item.getId(), clazz);
			results.add(result);
		}

		return results;
	}

	public <T> Page<T> mapResults(SearchScrollResult response, Class<T> clazz) {

		LinkedList<T> results = new LinkedList<>();

		for (SearchScrollResult.Hit<JsonObject, Void> hit : response.getHits(JsonObject.class)) {
			if (hit != null) {
				results.add(mapSource(hit.source, clazz));
			}
		}

		return new AggregatedPageImpl<>(results, Pageable.unpaged(), response.getTotal(), response.getScrollId());
	}

	public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, Pageable pageable) {
		return mapResults(response, clazz, null, pageable);
	}

	public <T> AggregatedPage<T> mapResults(SearchResult response, Class<T> clazz, List<AbstractAggregationBuilder> aggregations, Pageable pageable) {

		LinkedList<T> results = new LinkedList<>();

		for (SearchResult.Hit<JsonObject, Void> hit : response.getHits(JsonObject.class)) {
			if (hit != null) {
				T result = mapSource(hit.source, clazz);
				setPersistentEntityScore(result, hit.score, clazz);
				results.add(result);
			}
		}

		String scrollId = null;
		if (response instanceof ExtendedSearchResult) {
			scrollId = ((ExtendedSearchResult) response).getScrollId();
		}

		return new AggregatedPageImpl<>(results, pageable, response.getTotal(), response.getAggregations(), scrollId);
	}

	private <T> T mapSource(JsonObject source, Class<T> clazz) {
		String sourceString = source.toString();
		T result = null;
		if (!StringUtils.isEmpty(sourceString)) {
			result = mapEntity(sourceString, clazz);
			setPersistentEntityId(result, source.get(JestResult.ES_METADATA_ID).getAsString(), clazz);
		} else {
			//TODO(Fields results) : Map Fields results
			//result = mapEntity(hit.getFields().values(), clazz);
		}
		return result;
	}

	private <T> T mapEntity(Collection<DocumentField> values, Class<T> clazz) {
		return mapEntity(buildJSONFromFields(values), clazz);
	}

	private <T> T mapEntity(String source, Class<T> clazz) {
		if (!hasText(source)) {
			return null;
		}
		try {
			return entityMapper.mapToObject(source, clazz);
		} catch (IOException e) {
			throw new ElasticsearchException("failed to map source [ " + source + "] to class " + clazz.getSimpleName(), e);
		}
	}

	private String buildJSONFromFields(Collection<DocumentField> values) {
		JsonFactory nodeFactory = new JsonFactory();
		try (
				ByteArrayOutputStream stream = new ByteArrayOutputStream();
				JsonGenerator generator = nodeFactory.createGenerator(stream, JsonEncoding.UTF8);
		) {
			generator.writeStartObject();
			for (DocumentField value : values) {
				if (value.getValues().size() > 1) {
					generator.writeArrayFieldStart(value.getName());
					for (Object val : value.getValues()) {
						generator.writeObject(val);
					}
					generator.writeEndArray();
				} else {
					generator.writeObjectField(value.getName(), value.getValue());
				}
			}
			generator.writeEndObject();
			generator.flush();
			return new String(stream.toByteArray(), Charset.forName("UTF-8"));
		} catch (IOException e) {
			return null;
		}
	}

	private <T> void setPersistentEntityId(Object entity, String id, Class<T> clazz) {

		ElasticsearchPersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(clazz);
		ElasticsearchPersistentProperty idProperty = persistentEntity.getIdProperty();

		// Only deal with text because ES generated Ids are strings !
		if ((idProperty != null) && (idProperty.getType().isAssignableFrom(String.class))) {
			persistentEntity.getPropertyAccessor(entity).setProperty(idProperty, id);
		}
	}

	private <T> void setPersistentEntityScore(T result, double score, Class<T> clazz) {
		if (clazz.isAnnotationPresent(Document.class)) {
			ElasticsearchPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(clazz);

			if (entity.hasScoreProperty() && (entity.getScoreProperty() != null)) {
                entity.getPropertyAccessor(result).setProperty(entity.getScoreProperty(), (float) score);
			}
		}
	}
}
