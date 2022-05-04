package org.apache.flink.statefun.playground.java.graphanalytics.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TriangleQueryTrigger {
    @JsonProperty("vertex_id")
    private int vertexId;

    public TriangleQueryTrigger() {}

    /**
     * overloaded constructor
     * @param vertexId
     */
    private TriangleQueryTrigger(int vertexId) {
        this.vertexId = vertexId;
    }

    public int getVertexId() { return vertexId; }

    
    /**
     * This method is used for creating a new TriangleQueryTrigger, please call TriangleQueryTrigger.create(vertex_id, start)
     * to create an object of this class
     *
     * @param vertexId
     * @return TriangleQueryTrigger
     */
    public static TriangleQueryTrigger create(int vertexId) {
        return new TriangleQueryTrigger(vertexId);
    }
}
