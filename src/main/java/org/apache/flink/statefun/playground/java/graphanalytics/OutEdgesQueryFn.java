package org.apache.flink.statefun.playground.java.graphanalytics;

import org.apache.flink.statefun.playground.java.graphanalytics.types.*;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * This function processes the query for counting the number of outgoing edges of a vertex
 * In practice, there will be multiple logical instances of the OutEdgesQueryFn, and the number of logical
 * instances will be equal to the number of vertices in the graph. Each logical instance will be identified by the
 * address (OutEdgesQueryFn.TYPE_NAME, vertex_id). In this case, each logical instance only needs to store the incoming
 * edges for a specific vertex.
 * To send a query message to this function, please build a message with the OUT_EDGES_QUERY_TYPE in {@link Types} and
 * send to the address described above, where the vertex_id is the vertex we want to query
 */
public class OutEdgesQueryFn implements StatefulFunction {

    private static final ValueSpec<List<CustomTuple2<Integer, Long>>> OUT_NEIGHBORS =
            ValueSpec.named("outNeighbors").withCustomType(Types.OUT_NEIGHBORS_TYPE);

    static final TypeName TYPE_NAME = TypeName.typeNameOf("graph-analytics.fns", "outEdges");
    static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPE_NAME)
                    .withSupplier(OutEdgesQueryFn::new)
                    .withValueSpecs(OUT_NEIGHBORS)
                    .build();

    static final TypeName EGRESS_TYPE = TypeName.typeNameOf("graph-analytics.io", "egress");

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if (message.is(Types.Add_OUT_EDGE_TYPE)) {
            Vertex vertex = message.as(Types.Add_OUT_EDGE_TYPE);
            List<CustomTuple2<Integer, Long>> currentOutNeighbors = getCurrentOutNeighbors(context);
            sendRecommendCandidate(context, currentOutNeighbors, vertex.getDst());
            updateOutNeighbors(context, vertex, currentOutNeighbors);
            logOutNeighbors(vertex.getSrc(), context);
        } else if (message.is(Types.OUT_EDGES_QUERY_TYPE)) {
            OutEdgesQuery query = message.as(Types.OUT_EDGES_QUERY_TYPE);
            // the query we are implementing now is simple; it is only asking for all the incoming edges, so we can
            // just return the entire IN_NEIGHBORS list
            outputResult(context, query.getVertexId());
        } else if (message.is(Types.K_HOP_QUERY_TYPE)) {
            KHopQuery kHopQuery = message.as(Types.K_HOP_QUERY_TYPE);
            performKHop(context, kHopQuery);
        } else if (message.is(Types.TRIANGLE_QUERY_TRIGGER_TYPE)) {
            TriangleQueryTrigger trigger = message.as(Types.TRIANGLE_QUERY_TRIGGER_TYPE);
            triggerTriangleQuery(context, trigger);
          } else if (message.is(Types.TRIANGLE_QUERY_PASS_TYPE)) {
            TriangleQueryPass pass = message.as(Types.TRIANGLE_QUERY_PASS_TYPE);
            performTrianglePass(context, pass);
          }
        return context.done();
    }

    /**
     * This method returns the current outgoing neighbors of a vertex
     * @param context
     * @return IN_NEIGHBORS
     */
    public List<CustomTuple2<Integer, Long>> getCurrentOutNeighbors(Context context) {
        return context.storage().get(OUT_NEIGHBORS).orElse(new ArrayList<CustomTuple2<Integer, Long>>());
    }

    private void triggerTriangleQuery(Context context, TriangleQueryTrigger trigger) {
        List<CustomTuple2<Integer, Long>> currentOutNeighbors = getCurrentOutNeighbors(context);
        ArrayList<Integer> filteredNodes = new ArrayList<Integer>(0);
    
        for (int i = 0; i < currentOutNeighbors.size(); i++) {
          CustomTuple2<Integer, Long> node = currentOutNeighbors.get(i);
          int nodeIndex = node.getField(0);
    
          if (!filteredNodes.contains(nodeIndex)) {
            filteredNodes.add(nodeIndex);
          }
        }
        
        for (int i = 0; i < filteredNodes.size(); i++) {
          int nodeIndex = filteredNodes.get(i);
    
          TriangleQueryPass pass = TriangleQueryPass.create(
            trigger.getVertexId(),
            nodeIndex,
            filteredNodes
          );
    
          context.send(
              MessageBuilder.forAddress(OutEdgesQueryFn.TYPE_NAME, String.valueOf(nodeIndex))
                .withCustomType(Types.TRIANGLE_QUERY_PASS_TYPE, pass)
                .build()
            );
        }
      }
    
      private void performTrianglePass(Context context, TriangleQueryPass pass) {
        List<CustomTuple2<Integer, Long>> currentOutNeighbors = getCurrentOutNeighbors(context);
        List<Integer> filteredNodes = new ArrayList<Integer>(0);
        ArrayList<Integer> prevTrace = pass.getTrace();
    
        for (int i = 0; i < currentOutNeighbors.size(); i++) {
          CustomTuple2<Integer, Long> node = currentOutNeighbors.get(i);
          int nodeIndex = node.getField(0);
    
          if (!filteredNodes.contains(nodeIndex)) {
            filteredNodes.add(nodeIndex);
          }
        }
    
        for (int i = 0; i < filteredNodes.size(); i++) {
          int nodeIndex = filteredNodes.get(i);
    
          if (prevTrace.contains(nodeIndex)) {
            System.out.println(String.format("Found Outgoing Triangle for Vertex %d: (%d -> %d) (%d -> %d -> %d)", 
                  pass.getVertexId(), pass.getVertexId(), nodeIndex, pass.getVertexId(), pass.getCurrentId(), nodeIndex));
            
            outputTriangleResult(context, pass.getVertexId(), pass.getCurrentId(), nodeIndex);
          }
        }
      }

    private void performKHop(Context context, KHopQuery kHopQuery) {
        List<CustomTuple2<Integer, Long>> currentInNeighbors = getCurrentOutNeighbors(context);
        List<Integer> filteredNodes = new ArrayList<Integer>(0);
    
        for (int i = 0; i < currentInNeighbors.size(); i++) {
          CustomTuple2<Integer, Long> node = currentInNeighbors.get(i);
          int nodeIndex = node.getField(0);
    
          if (!kHopQuery.getTrace().contains(nodeIndex) && !filteredNodes.contains(nodeIndex)) {
            filteredNodes.add(nodeIndex);
          }
        }
    
        if (kHopQuery.getN() > 0) {
          ArrayList<Integer> trace = kHopQuery.getTrace();
          trace.add(kHopQuery.getCurrentId());
    
          for (int i = 0; i < filteredNodes.size(); i++) {
            KHopQuery nextQuery = KHopQuery.create(
                    kHopQuery.getVertexId(),
                    filteredNodes.get(i),
                    kHopQuery.getK(),
                    kHopQuery.getN() - 1,
                    trace
            );
    
            context.send(
              MessageBuilder.forAddress(OutEdgesQueryFn.TYPE_NAME, String.valueOf(filteredNodes.get(i)))
                .withCustomType(Types.K_HOP_QUERY_TYPE, nextQuery)
                .build()
            );
          }
        } else {
          for (int i = 0; i < filteredNodes.size(); i++) {
            System.out.println(String.format("Outgoing K-Hop node for vertex %d (K = %d): %d", kHopQuery.getVertexId(), kHopQuery.getK(), filteredNodes.get(i)));
            outputKHopResult(context, kHopQuery.getVertexId(), kHopQuery.getK(), filteredNodes.get(i));
          }
        }
      }

    /**
     * This method update the OUT_NEIGHBORS list by adding a new outgoing neighbor to the list
     * while ensuring that all the neighbors in the list are sorted by timestamp value
     * @param context
     * @param vertex
     * @param currentOutNeighbors
     */
    public void updateOutNeighbors(Context context, Vertex vertex, List<CustomTuple2<Integer, Long>> currentOutNeighbors) {
        CustomTuple2<Integer, Long> newOutNeighbor = CustomTuple2.createTuple2(vertex.getDst(), vertex.getTimestamp());
        // perform binary search to add incoming neighbor to the correct index, so that the IN_NEIGHBORS list remains
        // sorted by timestamp
        int left = 0, right = currentOutNeighbors.size() - 1;
        int insertIdx = 0;
        while (left <= right) {
            int mid = left + (right-left)/2;
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
   * This method sends potential recommendation candidates to the RecommendationFn,
   * we only choose a maximum of 3 out neighbors to recommend the newly added vertex to
   * @param context
   * @param currentOutNeighbors
   * @param vertexId
   */
    public void sendRecommendCandidate(Context context, List<CustomTuple2<Integer, Long>> currentOutNeighbors, int vertexId) {
        // if we have less than or equal to 3 out-neighbors, recommend the new vertex to all 3 of them
        if (currentOutNeighbors.size() <= 3) {
            for (CustomTuple2<Integer, Long> neighbor : currentOutNeighbors) {
                int targetVertex = neighbor.getField(0);
                context.send(
                    MessageBuilder.forAddress(RecommendationFn.TYPE_NAME, String.valueOf(targetVertex))
                        .withValue(vertexId)
                        .build()
                );
            }
        } else {
            // if we have more than 3 out-neighbors, randomly choose 3 vertices to recommend new vertex to
            int range = currentOutNeighbors.size();
            Random rand = new Random();
            for (int i = 0; i < 3; i++) {
                int chosenIdx = rand.nextInt(range);
                int targetVertex = currentOutNeighbors.get(chosenIdx).getField(0);
                context.send(
                    MessageBuilder.forAddress(RecommendationFn.TYPE_NAME, String.valueOf(targetVertex))
                        .withValue(vertexId)
                        .build()
                );
            }
        }
    }

    /**
     * This method outputs Triangle query result to egress.
     * @param context
     * @param vertexId
     * @param currentId
     * @param destId
     */

    private void outputTriangleResult(Context context, int vertexId, int currentId, int destId) {
        context.send(
            KafkaEgressMessage.forEgress(EGRESS_TYPE)
                .withTopic("out-triangle-result")
                .withUtf8Key(String.valueOf(vertexId))
                .withUtf8Value(String.format("Found Outgoing Triangle for Vertex %d: (%d -> %d) (%d -> %d -> %d)", 
                    vertexId, vertexId, destId, vertexId, currentId, destId))
                .build()
        );
    }

    /**
     * This method outputs k-hop query result to egress.
     * @param context
     * @param vertexId
     */

    private void outputKHopResult(Context context, int vertexId, int k, int currentId) {
    context.send(
        KafkaEgressMessage.forEgress(EGRESS_TYPE)
            .withTopic("out-k-hop-result")
            .withUtf8Key(String.valueOf(vertexId))
            .withUtf8Value(String.format("Outgoing K-Hop node for vertex %d (K = %d): %d", vertexId, k, currentId))
            .build()
        );
    }

    /**
     * This method outputs query result to egress.
     * @param context
     * @param vertexId
     */
    private void outputResult(Context context, int vertexId) {
        List<CustomTuple2<Integer, Long>> currentOutNeighbors =
                context.storage().get(OUT_NEIGHBORS).orElse(Collections.emptyList());

        context.send(
            KafkaEgressMessage.forEgress(EGRESS_TYPE)
                .withTopic("outgoing-edges")
                .withUtf8Key(String.valueOf(vertexId))
                .withUtf8Value(String.format("the outgoing edges of vertex %d are %s\n", vertexId, currentOutNeighbors))
                .build()
        );
    }

    /**
     * This methods prints out the current outgoing edges/neighbors of a vertex
     * @param vertex
     * @param context
     */
    private void logOutNeighbors(int vertex, Context context) {
        List<CustomTuple2<Integer, Long>> currentOutNeighbors = context.storage().get(OUT_NEIGHBORS).orElse(Collections.emptyList());
        System.out.printf("vertex %d currently has these outgoing neighbors: %s\n", vertex, currentOutNeighbors);
    }
}
