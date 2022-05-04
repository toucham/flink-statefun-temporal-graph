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
import java.util.concurrent.CompletableFuture;

/**
 * This function processes the query for counting the number of incoming edges of a vertex
 * In practice, there will be multiple logical instances of the InEdgesQueryFn, and the number of logical
 * instances will be equal to the number of vertices in the graph. Each logical instance will be identified by the
 * address (InEdgesQueryFn.TYPE_NAME, vertex_id). In this case, each logical instance only needs to store the incoming
 * edges for a specific vertex.
 * To send a query message to this function, please build a message with the IN_EDGES_QUERY_TYPE in {@link Types} and
 * send to the address described above, where the vertex_id is the vertex we want to query
 */
public class InEdgesQueryFn implements StatefulFunction {

  private static final ValueSpec<List<CustomTuple2<Integer, Long>>> IN_NEIGHBORS =
      ValueSpec.named("inNeighbors").withCustomType(Types.IN_NEIGHBORS_TYPE);
//  private static ValueSpec<Integer> Add_Edge_Count = ValueSpec.named("addEdgeCount").withIntType();
//  private static ValueSpec<Integer> Query_Count = ValueSpec.named("queryCount").withIntType();

  static final TypeName TYPE_NAME = TypeName.typeNameOf("graph-analytics.fns", "inEdges");
  static final StatefulFunctionSpec SPEC =
      StatefulFunctionSpec.builder(TYPE_NAME)
          .withSupplier(InEdgesQueryFn::new)
          .withValueSpecs(IN_NEIGHBORS)
          .build();

  static final TypeName EGRESS_TYPE = TypeName.typeNameOf("graph-analytics.io", "egress");

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
    if (message.is(Types.Add_IN_EDGE_TYPE)) {
      Vertex vertex = message.as(Types.Add_IN_EDGE_TYPE);
      List<CustomTuple2<Integer, Long>> currentInNeighbors = getCurrentInNeighbors(context);
      updateInNeighbors(context, vertex, currentInNeighbors);
      logInNeighbors(vertex.getDst(), context);
    } else if (message.is(Types.IN_EDGES_QUERY_TYPE)) {
      InEdgesQuery query = message.as(Types.IN_EDGES_QUERY_TYPE);
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
   * This method returns the current incoming neighbors of a vertex
   * @param context
   * @return IN_NEIGHBORS
   */
  public List<CustomTuple2<Integer, Long>> getCurrentInNeighbors(Context context) {
    return context.storage().get(IN_NEIGHBORS).orElse(new ArrayList<CustomTuple2<Integer, Long>>());
  }

  private void triggerTriangleQuery(Context context, TriangleQueryTrigger trigger) {
    List<CustomTuple2<Integer, Long>> currentInNeighbors = getCurrentInNeighbors(context);
    ArrayList<Integer> filteredNodes = new ArrayList<Integer>(0);

    for (int i = 0; i < currentInNeighbors.size(); i++) {
      CustomTuple2<Integer, Long> node = currentInNeighbors.get(i);
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
          MessageBuilder.forAddress(InEdgesQueryFn.TYPE_NAME, String.valueOf(nodeIndex))
            .withCustomType(Types.TRIANGLE_QUERY_PASS_TYPE, pass)
            .build()
        );
    }
  }

  private void performTrianglePass(Context context, TriangleQueryPass pass) {
    List<CustomTuple2<Integer, Long>> currentInNeighbors = getCurrentInNeighbors(context);
    ArrayList<Integer> filteredNodes = new ArrayList<Integer>(0);
    ArrayList<Integer> prevTrace = pass.getTrace();

    for (int i = 0; i < currentInNeighbors.size(); i++) {
      CustomTuple2<Integer, Long> node = currentInNeighbors.get(i);
      int nodeIndex = node.getField(0);

      if (!filteredNodes.contains(nodeIndex)) {
        filteredNodes.add(nodeIndex);
      }
    }

    for (int i = 0; i < filteredNodes.size(); i++) {
      int nodeIndex = filteredNodes.get(i);

      if (prevTrace.contains(nodeIndex)) {
        System.out.println(String.format("Found Incoming Triangle for Vertex %d: (%d -> %d) (%d -> %d -> %d)", 
              pass.getVertexId(), nodeIndex, pass.getVertexId(), nodeIndex, pass.getCurrentId(), pass.getVertexId()));
        
        outputTriangleResult(context, pass.getVertexId(), pass.getCurrentId(), nodeIndex);
      }
    }
  }

  private void performKHop(Context context, KHopQuery kHopQuery) {
    List<CustomTuple2<Integer, Long>> currentInNeighbors = getCurrentInNeighbors(context);
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
          MessageBuilder.forAddress(InEdgesQueryFn.TYPE_NAME, String.valueOf(filteredNodes.get(i)))
            .withCustomType(Types.K_HOP_QUERY_TYPE, nextQuery)
            .build()
        );
      }
    } else {
      for (int i = 0; i < filteredNodes.size(); i++) {
        System.out.println(String.format("Incoming K-Hop node for vertex %d (K = %d): %d", kHopQuery.getVertexId(), kHopQuery.getK(), filteredNodes.get(i)));
        outputKHopResult(context, kHopQuery.getVertexId(), kHopQuery.getK(), filteredNodes.get(i));
      }
    }
  }

  /**
   * This method update the IN_NEIGHBORS list by adding a new incoming neighbor to the list
   * while ensuring that all the neighbors in the list are sorted by timestamp value
   * @param context
   * @param vertex
   * @param currentInNeighbors
   */
  public void updateInNeighbors(Context context, Vertex vertex, List<CustomTuple2<Integer, Long>> currentInNeighbors) {
    CustomTuple2<Integer, Long> newInNeighbor = CustomTuple2.createTuple2(vertex.getSrc(), vertex.getTimestamp());
    // perform binary search to add incoming neighbor to the correct index, so that the IN_NEIGHBORS list remains
    // sorted by timestamp
    int left = 0, right = currentInNeighbors.size() - 1;
    int insertIdx = 0;
    while (left <= right) {
      int mid = left + (right-left)/2;
      Long t1 = currentInNeighbors.get(mid).getField(1);
      Long t2 = newInNeighbor.getField(1);
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
    currentInNeighbors.add(insertIdx, newInNeighbor);
    context.storage().set(IN_NEIGHBORS, currentInNeighbors);
  }


  /**
   * This method outputs Triangle query result to egress.
   * @param context
   * @param vertexId
   * @param currentId
   * @param sourceId
   */

  private void outputTriangleResult(Context context, int vertexId, int currentId, int sourceId) {
    context.send(
        KafkaEgressMessage.forEgress(EGRESS_TYPE)
            .withTopic("in-triangle-result")
            .withUtf8Key(String.valueOf(vertexId))
            .withUtf8Value(String.format("Found Incoming Triangle for Vertex %d: (%d -> %d) (%d -> %d -> %d)", 
                vertexId, sourceId, vertexId, sourceId, currentId, vertexId))
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
            .withTopic("in-k-hop-result")
            .withUtf8Key(String.valueOf(vertexId))
            .withUtf8Value(String.format("Incoming K-Hop node for vertex %d (K = %d): %d", vertexId, k, currentId))
            .build()
    );
  }

  /**
   * This method outputs query result to egress.
   * @param context
   * @param vertexId
   */
  private void outputResult(Context context, int vertexId) {
    List<CustomTuple2<Integer, Long>> currentInNeighbors =
        context.storage().get(IN_NEIGHBORS).orElse(Collections.emptyList());
    context.send(
        KafkaEgressMessage.forEgress(EGRESS_TYPE)
            .withTopic("incoming-edges")
            .withUtf8Key(String.valueOf(vertexId))
            .withUtf8Value(String.format("the incoming edges of vertex %d are %s\n", vertexId, currentInNeighbors))
            .build()
    );
  }

  /**
   * This methods prints out the current incoming edges/neighbors of a vertex
   * @param vertex
   * @param context
   */
  private void logInNeighbors(int vertex, Context context) {
    List<CustomTuple2<Integer, Long>> currentInNeighbors = context.storage().get(IN_NEIGHBORS).orElse(Collections.emptyList());

    System.out.printf("vertex %d currently has these incoming neighbors: %s\n", vertex, currentInNeighbors);
  }
}
