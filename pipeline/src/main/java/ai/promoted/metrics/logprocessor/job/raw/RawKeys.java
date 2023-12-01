package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.CohortMembership;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/** KeySelectors util for the Raw Job. */
interface RawKeys {
  // [platformId, dateHourUtcEpoch, userId, logUserId]
  KeySelector<LogUserUser, Tuple4<Long, Long, String, String>> logUserUserKeySelector =
      new KeySelector<LogUserUser, Tuple4<Long, Long, String, String>>() {
        @Override
        public Tuple4<Long, Long, String, String> getKey(LogUserUser logUserUser) {
          return Tuple4.of(
              logUserUser.getPlatformId(),
              // Round to hour.
              Instant.ofEpochMilli(logUserUser.getEventTimeMillis())
                  .truncatedTo(ChronoUnit.HOURS)
                  .toEpochMilli(),
              logUserUser.getUserId(),
              logUserUser.getLogUserId());
        }
      };

  // [platformId, requestId]
  KeySelector<DeliveryLog, Tuple2<Long, String>> deliveryLogKeySelector =
      new KeySelector<DeliveryLog, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(DeliveryLog deliveryLog) {
          return Tuple2.of(deliveryLog.getPlatformId(), deliveryLog.getRequest().getRequestId());
        }
      };

  // [platformId, membershipId]
  KeySelector<CohortMembership, Tuple2<Long, String>> cohortMembershipKeySelector =
      new KeySelector<CohortMembership, Tuple2<Long, String>>() {
        @Override
        public Tuple2<Long, String> getKey(CohortMembership cohortMembership) {
          return Tuple2.of(cohortMembership.getPlatformId(), cohortMembership.getMembershipId());
        }
      };
}
