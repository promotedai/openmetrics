package ai.promoted.metrics.logprocessor.common.queryablestate;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.OutputTag;

/**
 * This class create a new stream graph based on the original one and the service to export. It will
 * then add a new co-located node as the queryable service.
 */
public class StreamGraphRewriter {
  public static final String QUERY_OPERATOR_UID_PREFIX = "StateQuery-";

  public static void rewriteToAddQueryOperators(StreamGraph existing, List<String> uids) {
    if (null == uids || uids.isEmpty()) {
      return;
    }

    List<Tuple2<Integer, QueryServiceOperatorFactory>> operatorFactories = new ArrayList<>();
    Reflector reflector = new Reflector();
    for (String uid : uids) {
      StreamNode selectedNode = getStreamNodeByUid(existing, uid);

      checkState(selectedNode != null, "Node with uid " + uid + " not found");
      checkState(
          selectedNode.getStateKeySerializer() != null,
          "Node with uid " + uid + " doesn't have keyed state");

      String slotSharingGroup = selectedNode.getSlotSharingGroup();
      if (slotSharingGroup == null) {
        slotSharingGroup = UUID.randomUUID().toString();
        selectedNode.setSlotSharingGroup(slotSharingGroup);
      }

      String coLocationGroup = selectedNode.getCoLocationGroup();
      if (coLocationGroup == null) {
        coLocationGroup = UUID.randomUUID().toString();
        selectedNode.setCoLocationGroup(coLocationGroup);
      }

      // Here we want to inject the logic to register all the operators so that the querier could
      // get the operator at the Task side.
      // We do not wrap the operator factory since the operator factory class has too much feature
      // interface. It is hard to use a unified class to mock all the combination of interfaces.
      // Here we only set the task type of the selectNode, since selectNode must be the head
      // operator after keyBy(), thus its jobVertexClass will be effective
      // (There is one exception if reinterpretAsKeyedStream() is used but that should be not
      // possible in the current jobs).
      Class<? extends TaskInvokable> jobVertexClass =
          reflector.getFieldValue(selectedNode, "jobVertexClass");
      if (jobVertexClass.equals(OneInputStreamTask.class)) {
        reflector.setFieldValue(
            selectedNode, "jobVertexClass", SelfRegisterOneInputStreamTask.class);
      } else if (jobVertexClass.equals(TwoInputStreamTask.class)) {
        reflector.setFieldValue(
            selectedNode, "jobVertexClass", SelfRegisterTwoInputStreamTask.class);
      } else {
        throw new IllegalStateException("Not supported task type: " + jobVertexClass);
      }

      // create side-out for the selected node
      int virtualId = Transformation.getNewNodeId();
      existing.addVirtualSideOutputNode(
          selectedNode.getId(),
          virtualId,
          new OutputTag<Void>(String.format("%s-%s", uid, virtualId)) {});

      // create a corresponding node
      int newVertexId = Transformation.getNewNodeId();
      QueryServiceOperatorFactory operatorFactory =
          new QueryServiceOperatorFactory(
              uid,
              OperatorInfo.create(
                  selectedNode.getStateKeySerializer(),
                  selectedNode.getParallelism(),
                  existing.getExecutionConfig().getMaxParallelism() == -1
                      ? KeyGroupRangeAssignment.computeDefaultMaxParallelism(
                          selectedNode.getParallelism())
                      : existing.getExecutionConfig().getMaxParallelism()));
      existing.addOperator(
          newVertexId,
          slotSharingGroup,
          coLocationGroup,
          operatorFactory,
          BasicTypeInfo.VOID_TYPE_INFO,
          BasicTypeInfo.VOID_TYPE_INFO,
          QUERY_OPERATOR_UID_PREFIX + selectedNode.getOperatorName());
      existing.setParallelism(newVertexId, selectedNode.getParallelism());
      existing.setMaxParallelism(newVertexId, existing.getExecutionConfig().getMaxParallelism());

      // Colocation requires the same pipeline region.
      existing.addEdge(virtualId, newVertexId, 0);
      operatorFactories.add(new Tuple2<>(selectedNode.getId(), operatorFactory));
    }

    // we need also link the query operator to the target operator.
    StreamGraphHasherV2 hasher = new StreamGraphHasherV2();
    Map<Integer, byte[]> hashes = hasher.traverseStreamGraphAndGenerateHashes(existing);

    for (Tuple2<Integer, QueryServiceOperatorFactory> tuple2 : operatorFactories) {
      tuple2.f1.setTargetOperatorId(new OperatorID(hashes.get(tuple2.f0)));
    }
  }

  private static StreamNode getStreamNodeByUid(StreamGraph existing, String uid) {
    for (StreamNode node : existing.getStreamNodes()) {
      if (Objects.equals(node.getTransformationUID(), uid)) {
        return node;
      }
    }
    return null;
  }
}
