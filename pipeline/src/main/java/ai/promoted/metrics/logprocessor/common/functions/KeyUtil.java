package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil;
import ai.promoted.metrics.logprocessor.common.util.FlatUtil;
import ai.promoted.proto.common.AnonUserRetainedUser;
import ai.promoted.proto.common.RetainedUser;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AttributedAction;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.JoinedImpression;
import ai.promoted.proto.event.TinyAction;
import ai.promoted.proto.event.TinyActionPath;
import ai.promoted.proto.event.TinyAttributedAction;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyDeliveryLog;
import ai.promoted.proto.event.TinyImpression;
import ai.promoted.proto.event.TinyInsertion;
import ai.promoted.proto.event.TinyJoinedImpression;
import ai.promoted.proto.event.UnionEvent;
import ai.promoted.proto.event.View;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

/* Various KeySelectors ssince Flink has a hard time figuring out types of Tuples when using lambda. */
public interface KeyUtil {

  // [platformId, membershipId]
  KeySelector<CohortMembership, Tuple2<Long, String>> cohortMembershipKeySelector =
      new KeySelector<CohortMembership, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(CohortMembership cohortMembership) {
          return Tuple2.of(cohortMembership.getPlatformId(), cohortMembership.getMembershipId());
        }
      };

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

  // [platformId, anonUserId]
  KeySelector<TinyJoinedImpression, Tuple2<Long, String>> tinyJoinedImpressionAnonUserIdKey =
      new KeySelector<TinyJoinedImpression, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyJoinedImpression impression) {
          return Tuple2.of(
              impression.getImpression().getCommon().getPlatformId(),
              impression.getImpression().getCommon().getAnonUserId());
        }
      };

  // [platformId, anonUserId]
  KeySelector<JoinedImpression, Tuple2<Long, String>> joinedImpressionAnonUserIdKey =
      new KeySelector<JoinedImpression, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(JoinedImpression impression) {
          return Tuple2.of(
              impression.getIds().getPlatformId(), impression.getIds().getAnonUserId());
        }
      };

  KeySelector<TinyAttributedAction, Tuple2<Long, String>> tinyAttributedActionAnonUserIdKey =
      new KeySelector<TinyAttributedAction, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyAttributedAction action) {
          return Tuple2.of(
              action.getAction().getCommon().getPlatformId(),
              action.getAction().getCommon().getAnonUserId());
        }
      };

  KeySelector<TinyActionPath, Tuple2<Long, String>> tinyActionPathAnonUserIdKey =
      new KeySelector<TinyActionPath, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyActionPath actionPath) throws Exception {
          return tinyActionAnonUserIdKey.getKey(actionPath.getAction());
        }
      };

  KeySelector<AttributedAction, Tuple2<Long, String>> attributedActionAnonUserIdKey =
      new KeySelector<AttributedAction, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(AttributedAction action) {
          return Tuple2.of(
              action.getAction().getPlatformId(), action.getAction().getUserInfo().getAnonUserId());
        }
      };

  KeySelector<JoinedImpression, Tuple2<Long, String>> joinedImpressionKey =
      new KeySelector<JoinedImpression, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(JoinedImpression impression) {
          return Tuple2.of(
              impression.getIds().getPlatformId(), impression.getIds().getImpressionId());
        }
      };

  // Includes contentId because JoinedActions can be split by shopping cart content.
  // Prefers the contentId on the action's single_cart_item.  This might not match the
  // insertion's contentId.
  KeySelector<AttributedAction, Tuple4<Long, String, String, Long>> attributedActionKey =
      new KeySelector<AttributedAction, Tuple4<Long, String, String, Long>>() {
        @Override
        public Tuple4<Long, String, String, Long> getKey(AttributedAction attributedAction) {
          JoinedImpression joinedImpression =
              attributedAction.getTouchpoint().getJoinedImpression();
          return Tuple4.of(
              joinedImpression.getIds().getPlatformId(),
              attributedAction.getAction().getActionId(),
              FlatUtil.getContentIdPreferAction(attributedAction),
              attributedAction.getAttribution().getModelId());
        }
      };

  KeySelector<JoinedImpression, Tuple3<Long, String, String>> joinedImpressionAnonUserInsertionKey =
      new KeySelector<JoinedImpression, Tuple3<Long, String, String>>() {
        @Override
        public Tuple3<Long, String, String> getKey(JoinedImpression joinedImpression) {
          return Tuple3.of(
              joinedImpression.getIds().getPlatformId(),
              joinedImpression.getIds().getAnonUserId(),
              joinedImpression.getIds().getInsertionId());
        }
      };

  KeySelector<AttributedAction, Tuple3<Long, String, String>> attributedActionAnonUserInsertionKey =
      new KeySelector<AttributedAction, Tuple3<Long, String, String>>() {
        @Override
        public Tuple3<Long, String, String> getKey(AttributedAction attributedAction)
            throws Exception {
          return joinedImpressionAnonUserInsertionKey.getKey(
              attributedAction.getTouchpoint().getJoinedImpression());
        }
      };

  KeySelector<TinyCommonInfo, Tuple2<Long, String>> tinyCommon =
      new KeySelector<TinyCommonInfo, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyCommonInfo common) {
          return Tuple2.of(common.getPlatformId(), common.getAnonUserId());
        }
      };

  KeySelector<TinyDeliveryLog, Tuple2<Long, String>> tinyDeliveryLogAnonUserIdKey =
      new KeySelector<TinyDeliveryLog, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyDeliveryLog deliveryLog) throws Exception {
          return tinyCommon.getKey(deliveryLog.getCommon());
        }
      };

  KeySelector<TinyInsertion, Tuple2<Long, String>> tinyInsertionAnonUserIdKey =
      new KeySelector<TinyInsertion, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyInsertion insertion) throws Exception {
          return tinyCommon.getKey(insertion.getCommon());
        }
      };

  KeySelector<TinyImpression, Tuple2<Long, String>> tinyImpressionAnonUserIdKey =
      new KeySelector<TinyImpression, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyImpression impression) throws Exception {
          return tinyCommon.getKey(impression.getCommon());
        }
      };

  KeySelector<TinyAction, Tuple2<Long, String>> tinyActionAnonUserIdKey =
      new KeySelector<TinyAction, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyAction action) throws Exception {
          return tinyCommon.getKey(action.getCommon());
        }
      };

  KeySelector<TinyInsertion, Tuple2<Long, String>> tinyInsertionContentIdKey =
      new KeySelector<TinyInsertion, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyInsertion insertion) {
          return Tuple2.of(
              insertion.getCommon().getPlatformId(), insertion.getCore().getContentId());
        }
      };

  KeySelector<TinyAction, Tuple2<Long, String>> tinyActionContentIdKey =
      new KeySelector<TinyAction, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(TinyAction action) {
          return Tuple2.of(action.getCommon().getPlatformId(), action.getContentId());
        }
      };

  KeySelector<DeliveryLog, Tuple2<Long, String>> deliveryLogAnonUserIdKey =
      new KeySelector<DeliveryLog, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(DeliveryLog deliveryLog) {
          return Tuple2.of(
              DeliveryLogUtil.getPlatformId(deliveryLog),
              DeliveryLogUtil.getAnonUserId(deliveryLog));
        }
      };

  KeySelector<DeliveryLog, Tuple2<Long, String>> deliveryLogKeySelector =
      new KeySelector<DeliveryLog, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(DeliveryLog deliveryLog) {
          return Tuple2.of(
              DeliveryLogUtil.getPlatformId(deliveryLog), deliveryLog.getRequest().getRequestId());
        }
      };

  KeySelector<UnionEvent, Tuple2<Long, String>> unionEntityKeySelector =
      new KeySelector<UnionEvent, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(UnionEvent event) throws Exception {
          if (event.hasCombinedDeliveryLog()) {
            return Tuple2.of(
                DeliveryLogUtil.getPlatformId(event.getCombinedDeliveryLog()),
                DeliveryLogUtil.getAnonUserId(event.getCombinedDeliveryLog()));
          } else if (event.hasImpression()) {
            return Tuple2.of(
                event.getImpression().getPlatformId(),
                event.getImpression().getUserInfo().getAnonUserId());
          } else if (event.hasJoinedImpression()) {
            return Tuple2.of(
                event.getJoinedImpression().getIds().getPlatformId(),
                event.getJoinedImpression().getIds().getAnonUserId());
          } else if (event.hasAction()) {
            return Tuple2.of(
                event.getAction().getPlatformId(), event.getAction().getUserInfo().getAnonUserId());
          } else {
            throw new UnsupportedOperationException("Unsupported UnionEvent type");
          }
        }
      };

  // TODO - For the prototype runs, we want to key by each of these separately.
  // [platformId, userId]
  KeySelector<RetainedUser, Tuple2<Long, String>> retainedUserKeySelector =
      new KeySelector<RetainedUser, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(RetainedUser retainedUser) {
          return Tuple2.of(retainedUser.getPlatformId(), retainedUser.getUserId());
        }
      };

  static long truncateToDateHourEpochMillis(long timestamp) {
    return Instant.ofEpochMilli(timestamp)
        .atZone(ZoneOffset.UTC)
        .truncatedTo(ChronoUnit.HOURS)
        .toInstant()
        .toEpochMilli();
  }

  // TODO - For the prototype runs, we want to key by each of these separately.
  // [platformId, logUserId, userId]
  KeySelector<LogUserUser, Tuple3<Long, String, String>> logUserUserKeySelector =
      new KeySelector<LogUserUser, Tuple3<Long, String, String>>() {
        @Override
        public Tuple3<Long, String, String> getKey(LogUserUser logUserUser) {
          return Tuple3.of(
              logUserUser.getPlatformId(), logUserUser.getLogUserId(), logUserUser.getUserId());
        }
      };

  // So we can deduplicate inside an hour.
  // [platformId, logUserId, userId, Long]
  KeySelector<LogUserUser, Tuple4<Long, String, String, Long>> logUserUserHourKeySelector =
      new KeySelector<LogUserUser, Tuple4<Long, String, String, Long>>() {
        @Override
        public Tuple4<Long, String, String, Long> getKey(LogUserUser logUserUser) {
          long hourEpochMillis = truncateToDateHourEpochMillis(logUserUser.getEventTimeMillis());
          return Tuple4.of(
              logUserUser.getPlatformId(),
              logUserUser.getLogUserId(),
              logUserUser.getUserId(),
              hourEpochMillis);
        }
      };

  TypeInformation<Tuple2<Long, String>> logUserIdKeyType = Types.TUPLE(Types.LONG, Types.STRING);

  // [platformId, anonUserId, retainedUserId]
  KeySelector<AnonUserRetainedUser, Tuple3<Long, String, String>> anonUserRetainedUserKeySelector =
      new KeySelector<AnonUserRetainedUser, Tuple3<Long, String, String>>() {
        @Override
        public Tuple3<Long, String, String> getKey(AnonUserRetainedUser row) {
          return Tuple3.of(row.getPlatformId(), row.getAnonUserId(), row.getRetainedUserId());
        }
      };

  // So we can deduplicate inside an hour.
  // [platformId, logUserId, userId, Long]
  KeySelector<AnonUserRetainedUser, Tuple4<Long, String, String, Long>>
      anonUserRetainedUserHourKeySelector =
          new KeySelector<AnonUserRetainedUser, Tuple4<Long, String, String, Long>>() {
            @Override
            public Tuple4<Long, String, String, Long> getKey(
                AnonUserRetainedUser anonUserRetainedUser) {
              long hourEpochMillis =
                  truncateToDateHourEpochMillis(anonUserRetainedUser.getEventTimeMillis());
              return Tuple4.of(
                  anonUserRetainedUser.getPlatformId(),
                  anonUserRetainedUser.getAnonUserId(),
                  anonUserRetainedUser.getRetainedUserId(),
                  hourEpochMillis);
            }
          };

  // TODO - might be safer to have ValidationError have a primary key field that is filled in by the
  // validator.
  private static String getPrimaryKey(ValidationError validationError) {
    switch (validationError.getRecordType()) {
      case COHORT_MEMBERSHIP:
        return validationError.getCohortMembershipId();
      case VIEW:
        return validationError.getViewId();
      case DELIVERY_LOG:
        return validationError.getRequestId();
      case REQUEST:
        return validationError.getRequestId();
      case REQUEST_INSERTION:
        return validationError.getResponseInsertionId();
      case EXECUTION_INSERTION:
        return validationError.getResponseInsertionId();
      case RESPONSE_INSERTION:
        return validationError.getResponseInsertionId();
      case IMPRESSION:
        return validationError.getImpressionId();
      case ACTION:
        return validationError.getActionId();
      case DIAGNOSTICS:
        // There's no primary key for Diagnostics.
        // TODO - add a primary key for Diagnostics.
        return Long.toString(validationError.getTiming().getEventApiTimestamp());
      default:
        throw new UnsupportedOperationException("recordType=" + validationError.getRecordType());
    }
  }

  // [platformId, recordType, errorType, field, primaryKey]
  KeySelector<ValidationError, Tuple5<Long, String, String, String, String>>
      validationErrorKeySelector =
          new KeySelector<ValidationError, Tuple5<Long, String, String, String, String>>() {
            @Override
            public Tuple5<Long, String, String, String, String> getKey(
                ValidationError validationError) {
              return Tuple5.of(
                  validationError.getPlatformId(),
                  validationError.getRecordType().toString(),
                  validationError.getErrorType().toString(),
                  validationError.getField().toString(),
                  getPrimaryKey(validationError));
            }
          };
}
