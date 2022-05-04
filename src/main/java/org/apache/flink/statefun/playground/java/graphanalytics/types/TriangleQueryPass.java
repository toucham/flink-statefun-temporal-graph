package org.apache.flink.statefun.playground.java.graphanalytics.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;

public class TriangleQueryPass {
    @JsonProperty("vertex_id")
    private int vertexId;

    @JsonProperty("current_id")
    private int currentId;

    @JsonProperty("trace")
    private ArrayList<Integer> trace;


    public TriangleQueryPass() {}

    /**
     * overloaded constructor
     * @param vertexId
     * @param currentId
     * @param trace
     */
    private TriangleQueryPass(int vertexId, int currentId, ArrayList<Integer> trace) {
        this.vertexId = vertexId;
        this.currentId = currentId;
        this.trace = trace;
    }

    public int getVertexId() { return vertexId; }

    public int getCurrentId() { return currentId; }

    public ArrayList<Integer> getTrace() { return trace; }

    /**
     * This method is used for creating a new TriangleQueryPass, please call TriangleQueryPass.create(vertex_id, current_id, trace, start)
     * to create an object of this class
     *
     * @param vertexId
     * @param currentId
     * @param trace
     * @return TriangleQueryPass
     */
    public static TriangleQueryPass create(int vertexId, int currentId, ArrayList<Integer> trace) {
        return new TriangleQueryPass(vertexId, currentId, trace);
    }
}
