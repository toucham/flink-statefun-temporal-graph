package org.apache.flink.statefun.playground.java.graphanalytics;

import org.apache.flink.statefun.playground.java.graphanalytics.types.*;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This function processes the query for counting the number of outgoing edges
 * of a vertex during a specified time window from the request parameters.
 * In practice, there will be multiple logical instances of the
 * TimeWindowQueryFn,
 * and the number of logical
 * instances will be <= the number of vertices in the graph. Each logical
 * instance will be identified by the
 * address (TimeWindowQueryFn.TYPE_NAME, src, startTime, endTime).
 * To send a query message to this function, please build a message with the
 * TIME_WINDOW_QUERY_TYPE in {@link Types} and
 * send to the address described above, where src is the vertex we
 * want to query, startTime is the start time of the window, and endTime is the
 * end time of the window
 */
public class TimeWindowQueryFn implements StatefulFunction {
    // stores outgoing edges list for source
    private static final ValueSpec<List<CustomTuple2<Integer, Long>>> OUT_NEIGHBORS = ValueSpec.named("outNeighbors")
            .withCustomType(Types.OUT_NEIGHBORS_TYPE);

    // type for TimeWindowQuery
    static final TypeName TYPE_NAME = TypeName.typeNameOf("graph-analytics.fns", "timeWindow");
    static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(TimeWindowQueryFn::new)
            .withValueSpecs(OUT_NEIGHBORS)
            .build();

    // egress output
    static final TypeName EGRESS_TYPE = TypeName.typeNameOf("graph-analytics.io", "egress");

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        // message handler to build local state
        if (message.is(Types.Add_OUT_EDGE_TYPE)) {
            Vertex vertex = message.as(Types.Add_OUT_EDGE_TYPE);
            List<CustomTuple2<Integer, Long>> currentOutNeighbors = getCurrentOutNeighbors(context);
            updateOutNeighbors(context, vertex, currentOutNeighbors);
            logOutNeighbors(vertex.getSrc(), context);
        }
        // message handler to compute time window nodes
        else if (message.is(Types.TIME_WINDOW_QUERY_TYPE)) {
            TimeWindowQuery query = message.as(Types.TIME_WINDOW_QUERY_TYPE);
            // get the nodes in the time window
            List<CustomTuple2<Integer, Long>> result = getTimeWindowNodes(context, query.getStartTime(),
                    query.getEndTime());
            System.out.printf("(time-window) Edges between [%d, %d]: \n%s\n", query.getStartTime(), query.getEndTime(),
                    result);
            outputResult(context, query.getSrc(), result);
        }
        return context.done();
    }

    /**
     * This method returns the current outgoing neighbors of a vertex
     * 
     * @param context
     * @return IN_NEIGHBORS
     */
    private List<CustomTuple2<Integer, Long>> getCurrentOutNeighbors(Context context) {
        return context.storage().get(OUT_NEIGHBORS).orElse(new ArrayList<CustomTuple2<Integer, Long>>());
    }

    /**
     * This method uses window formed by the two time parameters to return nodes
     * that were edges of the source node between that time window frame
     * 
     * @param context   current local context
     * @param startTime start time of the window
     * @param endTime   end time of the window
     * @return list of tuple2s (<id, timestamp>) of nodes during that window
     */
    private List<CustomTuple2<Integer, Long>> getTimeWindowNodes(Context context, long startTime, long endTime) {
        List<CustomTuple2<Integer, Long>> output = new ArrayList<>();
        List<CustomTuple2<Integer, Long>> outgoingEdges = getCurrentOutNeighbors(context);

        int startIndex = 0, left = 0, right = outgoingEdges.size() - 1;
        // binary search but garauntee that the insertion index is at
        // the FIRST number >= startTime
        while (left <= right) {
            int mid = left + (right - left) / 2;

            // if mid - 1 is less than 0, we check directly so
            // we dont get out of bounds exception
            if (mid - 1 < 0) {
                if ((long) outgoingEdges.get(mid).getField(1) >= startTime) {
                    startIndex = mid;
                    break;
                }
            } else if ((long) outgoingEdges.get(mid).getField(1) >= startTime
                    && (long) outgoingEdges.get(mid - 1).getField(1) < startTime) {
                startIndex = mid;
                break;
            } else if ((long) outgoingEdges.get(mid).getField(1) >= startTime
                    && (long) outgoingEdges.get(mid - 1).getField(1) > startTime) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        // add the tuples btwn startTime and endTime
        for (int i = startIndex; i < outgoingEdges.size(); i++) {
            CustomTuple2<Integer, Long> tup = outgoingEdges.get(i);
            if ((long) tup.getField(1) <= endTime) {
                output.add(outgoingEdges.get(i));
            } else {
                break;
            }
        }

        return output;
    }

    /**
     * This method update the OUT_NEIGHBORS list by adding a new outgoing neighbor
     * to the list while ensuring that all the neighbors in the list are sorted by
     * timestamp value
     * 
     * @param context
     * @param vertex
     * @param currentOutNeighbors
     */
    private void updateOutNeighbors(Context context, Vertex vertex,
            List<CustomTuple2<Integer, Long>> currentOutNeighbors) {
        @SuppressWarnings("unchecked")
        CustomTuple2<Integer, Long> newOutNeighbor = CustomTuple2.createTuple2(vertex.getDst(), vertex.getTimestamp());
        // perform binary search to add incoming neighbor to the correct index, so that
        // the IN_NEIGHBORS list remains
        // sorted by timestamp
        int left = 0, right = currentOutNeighbors.size() - 1;
        int insertIdx = 0;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            Long t1 = currentOutNeighbors.get(mid).getField(1);
            Long t2 = newOutNeighbor.getField(1);
            int comparison = t1.compareTo(t2);
            if (comparison == 0) {
                insertIdx = mid;
                break;
            } else if (comparison < 0) {
                left = mid + 1;
                insertIdx = left;
            } else {
                right = mid - 1;
            }
        }
        currentOutNeighbors.add(insertIdx, newOutNeighbor);
        context.storage().set(OUT_NEIGHBORS, currentOutNeighbors);
    }

    /**
     * This method outputs query result to egress.
     * 
     * @param context
     * @param vertexId
     */
    private void outputResult(Context context, int vertexId, List<CustomTuple2<Integer, Long>> result) {

        context.send(
            KafkaEgressMessage.forEgress(EGRESS_TYPE)
                .withTopic("time-window")
                .withUtf8Key(String.valueOf(vertexId))
                .withUtf8Value(String.format("The outgoing edges of vertex %s are %s\n", vertexId,
                    result))
                .build()
        );
    }

    /**
     * This methods prints out the current outgoing edges/neighbors of a vertex
     * 
     * @param vertex
     * @param context
     */
    private void logOutNeighbors(int vertex, Context context) {
        List<CustomTuple2<Integer, Long>> currentOutNeighbors = context.storage().get(OUT_NEIGHBORS)
                .orElse(Collections.emptyList());

        System.out.printf("[time-window] vertex %d currently has these outgoing neighbors: %s\n", vertex,
                currentOutNeighbors);
    }
}
