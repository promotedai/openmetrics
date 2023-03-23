package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.FlatResponseInsertion;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedEvent;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyEvent;
import ai.promoted.proto.event.UnionEvent;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/* Various KeySelectors ssince Flink has a hard time figuring out types of Tuples when using lambda. */
public interface KeyUtil {

  // [platformId, viewId]
  KeySelector<View, Tuple2<Long, String>> viewKeySelector =
      new KeySelector<View, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(View view) {
          return Tuple2.of(view.getPlatformId(), view.getViewId());
        }
      };

  // [platformId, autoViewId]
  KeySelector<AutoView, Tuple2<Long, String>> autoViewKeySelector =
      new KeySelector<AutoView, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(AutoView autoView) {
          return Tuple2.of(autoView.getPlatformId(), autoView.getAutoViewId());
        }
      };

  // [platformId, impressionId]
  KeySelector<Impression, Tuple2<Long, String>> impressionKeySelector =
      new KeySelector<Impression, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(Impression impression) {
          return Tuple2.of(impression.getPlatformId(), impression.getImpressionId());
        }
      };

  // [platformId, actionId]
  KeySelector<Action, Tuple2<Long, String>> actionKeySelector =
      new KeySelector<Action, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(Action action) {
          return Tuple2.of(action.getPlatformId(), action.getActionId());
        }
      };

  KeySelector<JoinedEvent, Tuple2<Long, String>> joinedEventLogUserIdKey =
      new KeySelector<JoinedEvent, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(JoinedEvent event) {
          return Tuple2.of(FlatUtil.getPlatformId(event), FlatUtil.getLogUserId(event));
        }
      };

  KeySelector<JoinedEvent, Tuple2<Long, String>> joinedImpressionKey =
      new KeySelector<JoinedEvent, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(JoinedEvent event) {
          return Tuple2.of(FlatUtil.getPlatformId(event), event.getIds().getImpressionId());
        }
      };

  KeySelector<JoinedEvent, Tuple3<Long, String, String>> joinedEventLogUserInsertionKey =
      new KeySelector<JoinedEvent, Tuple3<Long, String, String>>() {
        @Override
        public Tuple3<Long, String, String> getKey(JoinedEvent event) {
          return Tuple3.of(
              FlatUtil.getPlatformId(event),
              FlatUtil.getLogUserId(event),
              FlatUtil.getInsertionId(event));
        }
      };

  KeySelector<FlatResponseInsertion, Tuple2<Long, String>> flatResponseInsertionLogUserIdKey =
      new KeySelector<FlatResponseInsertion, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(FlatResponseInsertion flat) throws Exception {
          return Tuple2.of(flat.getIds().getPlatformId(), flat.getIds().getLogUserId());
        }
      };

  KeySelector<TinyEvent, Tuple2<Long, String>> TinyEventLogUserIdKey =
      new KeySelector<TinyEvent, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyEvent event) {
          return Tuple2.of(event.getPlatformId(), event.getLogUserId());
        }
      };

  KeySelector<TinyEvent, Tuple2<Long, String>> TinyEventContentIdKey =
      new KeySelector<TinyEvent, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyEvent event) {
          return Tuple2.of(event.getPlatformId(), event.getContentId());
        }
      };

  // Keep as an anonymous class since Flink has a hard time figuring out types of Tuples when using
  // lambda.
  KeySelector<TinyDeliveryLog, Tuple2<Long, String>> TinyDeliveryLogLogUserIdKey =
      new KeySelector<TinyDeliveryLog, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyDeliveryLog event) {
          return Tuple2.of(event.getPlatformId(), event.getLogUserId());
        }
      };

  KeySelector<DeliveryLog, Tuple2<Long, String>> deliveryLogLogUserIdKey =
      new KeySelector<DeliveryLog, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(DeliveryLog deliveryLog) {
          return Tuple2.of(
              DeliveryLogUtil.getPlatformId(deliveryLog),
              DeliveryLogUtil.getLogUserId(deliveryLog));
        }
      };

  KeySelector<CombinedDeliveryLog, Tuple2<Long, String>> combinedDeliveryLogLogUserIdKey =
      new KeySelector<CombinedDeliveryLog, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(CombinedDeliveryLog deliveryLog) {
          return Tuple2.of(
              DeliveryLogUtil.getPlatformId(deliveryLog),
              DeliveryLogUtil.getLogUserId(deliveryLog));
        }
      };

  // Keep as an anonymous class since Flink has a hard time figuring out types of Tuples when using
  // lambda.
  KeySelector<User, Tuple2<Long, String>> logUserIdKeySelector =
      new KeySelector<User, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(User user) throws Exception {
          return Tuple2.of(user.getPlatformId(), user.getUserInfo().getLogUserId());
        }
      };

  KeySelector<UnionEvent, Tuple2<Long, String>> unionEntityKeySelector =
      new KeySelector<UnionEvent, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(UnionEvent event) throws Exception {
          if (event.hasView()) {
            return Tuple2.of(
                event.getView().getPlatformId(), event.getView().getUserInfo().getLogUserId());
          } else if (event.hasCombinedDeliveryLog()) {
            return Tuple2.of(
                DeliveryLogUtil.getPlatformId(event.getCombinedDeliveryLog()),
                DeliveryLogUtil.getLogUserId(event.getCombinedDeliveryLog()));
          } else if (event.hasImpression()) {
            return Tuple2.of(
                event.getImpression().getPlatformId(),
                event.getImpression().getUserInfo().getLogUserId());
          } else if (event.hasJoinedImpression()) {
            return Tuple2.of(
                event.getJoinedImpression().getIds().getPlatformId(),
                event.getJoinedImpression().getIds().getLogUserId());
          } else if (event.hasAction()) {
            return Tuple2.of(
                event.getAction().getPlatformId(), event.getAction().getUserInfo().getLogUserId());
          } else {
            throw new UnsupportedOperationException("Unsupported UnionEvent type");
          }
        }
      };

  TypeInformation<Tuple2<Long, String>> logUserIdKeyType = Types.TUPLE(Types.LONG, Types.STRING);
}
