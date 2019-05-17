package com.github.vanroy.springdata.jest.entities;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Score;

/**
 * @author yvlasiuk
 * @version 1.0
 */

@Data
@Document(indexName = "test-index-1", type = "score")
public class ScoreEntity {

    @Id
    private String id;
    private String message;
    @Score
    private Float score;

    public ScoreEntity() {
    }

    public ScoreEntity(String id, String message) {
        this.id = id;
        this.message = message;
    }
}
