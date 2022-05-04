package org.apache.flink.statefun.playground.java.graphanalytics;

import org.apache.flink.statefun.playground.java.graphanalytics.types.*;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.Collections;

import java.util.HashSet;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * This function processes the query for retrieving the recommendation candidates of a specific vertex
 * In practice, there will be multiple logical instances of the RecommendationFn, and the number of logical
 * instances will be approximately equal to the number of vertices in the graph. Each logical instance will be identified by the
 * address (RecommendationFn.TYPE_NAME, vertex_id). In this case, each logical instance only needs to store the recommendation
 * candidates of a specific vertex.
 * To send a query message to this function, please build a message with the RECOMMEND_QUERY_TYPE in {@link Types} and
 * send to the address described above, where the vertex_id is the vertex we want to query
 */
public class RecommendationFn implements StatefulFunction {

  private static final ValueSpec<Set<Integer>> RECOMMEND_SET =
    ValueSpec.named("recommendSet").withCustomType(Types.RECOMMEND_SET_TYPE);

  static final TypeName TYPE_NAME = TypeName.typeNameOf("graph-analytics.fns", "recommendation");

  static final StatefulFunctionSpec SPEC =
      StatefulFunctionSpec.builder(TYPE_NAME)
          .withSupplier(RecommendationFn::new)
          .withValueSpecs(RECOMMEND_SET)
          .build();



  static final TypeName EGRESS_TYPE = TypeName.typeNameOf("graph-analytics.io", "egress");



  @Override
  public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
    if (message.is(Types.RECOMMEND_QUERY_TYPE)){
      RecommendQuery query = message.as(Types.RECOMMEND_QUERY_TYPE);
      // since we've already stored a set of possible recommendation candidate, we can directly output the result
      outputResult(context, query.getVertexId());
    } else if (message.isInt()) {
      // the OutEdgesQuery function has forwarded a potential recommendation candidate to the recommendation function
      int candidate = message.asInt();
      if (candidate != -1) {
        updateRecommendSet(context, candidate);
      }
      System.out.println("received recommendation candidate");
    }
    return context.done();
  }

  /**
   * This method retrieves the current set of recommendation candidates
   * @param context
   * @return
   */
  public Set<Integer> getRecommendationSet(Context context) {
    return context.storage().get(RECOMMEND_SET).orElse(Collections.emptySet());
  }

  /**
   * This method updates the current recommendation set
   * @param context
   * @param candidate
   */
  public void updateRecommendSet(Context context, int candidate) {
    Set<Integer> curRecommendSet = getRecommendationSet(context);
    HashSet<Integer> newRecommendSet = new HashSet<>(curRecommendSet);
    // check if candidate is already in the recommendation set
    if (!curRecommendSet.contains(candidate)) {
      newRecommendSet.add(candidate);
      context.storage().set(RECOMMEND_SET, newRecommendSet);
    }
  }


  /**
   * This method outputs recommendation query results to egress
   * @param context
   * @param vertexId
   */
  private void outputResult(Context context, int vertexId) {
    Set<Integer> recommendSet = getRecommendationSet(context);
    context.send(
        KafkaEgressMessage.forEgress(EGRESS_TYPE)
            .withTopic("recommendation")
            .withUtf8Key(String.valueOf(vertexId))
            .withUtf8Value(String.format("recommend %s to vertex %d\n", recommendSet, vertexId))
            .build()
    );
  }
}
