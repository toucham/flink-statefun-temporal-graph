package org.apache.flink.statefun.playground.java.graphanalytics.types;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class defines the type of the query for counting the number of outgoing
 * edges of a node within a time frame
 * The function that is responsible for dispatching queries should send a
 * message of this type to the TimeWindowQueryFn.
 */
public class TimeWindowQuery {
    @JsonProperty("src")
    private int src;

    @JsonProperty("startTime")
    private long start;

    @JsonProperty("endTime")
    private long end;

    public TimeWindowQuery() {
    }

    /**
     * overloaded constructor
     * 
     * @param src
     * @param start
     * @param end
     */
    public TimeWindowQuery(int src, long start, long end) {
        this.src = src;
        this.start = start;
        this.end = end;
    }

    public int getSrc() {
        return src;
    }

    public long getStartTime() {
        return start;
    }

    public long getEndTime() {
        return end;
    }

    /**
     * This method is used for creating a new TimeWindowQuery, please call
     * TimeWindowQuery.create(src, start, end) to create an object of this class
     * 
     * @param src
     * @param start
     * @param end
     * @return TimeWindowQuery
     */
    public static TimeWindowQuery create(int src, long start, long end) {
        return new TimeWindowQuery(src, start, end);
    }
}
