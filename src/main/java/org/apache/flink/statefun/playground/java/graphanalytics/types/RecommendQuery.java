package org.apache.flink.statefun.playground.java.graphanalytics.types;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class defines the type of the query that gets recommendation candidates for a specific vertex
 * The function that is responsible for dispatching queries should send a message of this type to the RecommendationFn
 */
public class RecommendQuery {

    @JsonProperty("vertex_id")
    private int vertexId;

    @JsonProperty("t")
    private long timestamp;

    public RecommendQuery() {}

    /**
     * overloaded constructor
     * @param vertexId
     * @param timestamp
     */
    private RecommendQuery(int vertexId, long timestamp) {
        this.vertexId = vertexId;
        this.timestamp = timestamp;
    }

    public void setVertexId(int vertexId) {
        this.vertexId = vertexId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getVertexId() { return vertexId; }

    public long getTimestamp() { return timestamp; }

    /**
     * This method is used for creating a new InEdgesQuery, please call InEdgesQuery.create(vertex_id, timestamp)
     * to create an object of this class
     *
     * @param vertexId
     * @param timestamp
     * @return InEdgesQuery
     */
    public static RecommendQuery create(int vertexId, long timestamp) {
        return new RecommendQuery(vertexId, timestamp);
    }
}
