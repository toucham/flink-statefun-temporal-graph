package org.apache.flink.statefun.playground.java.graphanalytics.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;

/**
 * This class defines the type of the query for counting the number of incoming edges of a vertex.
 * The function that is responsible for dispatching queries should send a message of this type to the InEdgesQueryFn/OutEdgesQueryFn.
 */
public class KHopQuery {

    @JsonProperty("vertex_id")
    private int vertexId;

    @JsonProperty("current_id")
    private int currentId;

    @JsonProperty("k")
    private int k;

    @JsonProperty("n")
    private int n;

    @JsonProperty("trace")
    private ArrayList<Integer> trace;


    public KHopQuery() {}

    /**
     * overloaded constructor
     * @param vertexId
     * @param currentId
     * @param k
     * @param n
     * @param trace
     */
    private KHopQuery(int vertexId, int currentId, int k, int n, ArrayList<Integer> trace) {
        this.vertexId = vertexId;
        this.currentId = currentId;
        this.k = k;
        this.n = n;
        this.trace = trace;
    }

    public int getVertexId() { return vertexId; }

    public int getCurrentId() { return currentId; }

    public int getK() { return k; }

    public int getN() { return n; }

    public ArrayList<Integer> getTrace() { return trace; }

    /**
     * This method is used for creating a new KHopQuery, please call KHopQuery.create(vertex_id, timestamp)
     * to create an object of this class
     *
     * @param vertexId
     * @param k
     * @param n
     * @param trace
     * @return KHopQuery
     */
    public static KHopQuery create(int vertexId, int currentId, int k, int n, ArrayList<Integer> trace) {
        return new KHopQuery(vertexId, currentId, k, n, trace);
    }
}
