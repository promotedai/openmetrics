package ai.promoted.metrics.logprocessor.job.raw;

import ai.promoted.metrics.common.LogUserUser;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/** Converts LogRequest to join table LogUserUser records. */
class ToLogUserUser implements FlatMapFunction<LogRequest, LogUserUser> {

  @Override
  public void flatMap(LogRequest logRequest, Collector<LogUserUser> collector) throws Exception {
    long platformId = logRequest.getPlatformId();
    long eventApiTimestamp = logRequest.getTiming().getEventApiTimestamp();
    output(platformId, eventApiTimestamp, logRequest.getUserInfo(), collector);
    logRequest
        .getUserList()
        .forEach(user -> output(platformId, eventApiTimestamp, user, collector));
    logRequest
        .getCohortMembershipList()
        .forEach(
            cohortMembership -> output(platformId, eventApiTimestamp, cohortMembership, collector));
    logRequest
        .getViewList()
        .forEach(view -> output(platformId, eventApiTimestamp, view, collector));
    logRequest
        .getAutoViewList()
        .forEach(autoView -> output(platformId, eventApiTimestamp, autoView, collector));
    logRequest
        .getDeliveryLogList()
        .forEach(deliveryLog -> output(platformId, eventApiTimestamp, deliveryLog, collector));
    logRequest
        .getImpressionList()
        .forEach(impression -> output(platformId, eventApiTimestamp, impression, collector));
    logRequest
        .getActionList()
        .forEach(action -> output(platformId, eventApiTimestamp, action, collector));
    logRequest
        .getDiagnosticsList()
        .forEach(diagnostics -> output(platformId, eventApiTimestamp, diagnostics, collector));
  }

  private void output(
      long platformId, long eventApiTimestamp, User user, Collector<LogUserUser> collector) {
    if (user.getPlatformId() != 0L) {
      platformId = user.getPlatformId();
    }
    if (user.getTiming().getEventApiTimestamp() != 0L) {
      eventApiTimestamp = user.getTiming().getEventApiTimestamp();
    }
    output(platformId, eventApiTimestamp, user.getUserInfo(), collector);
  }

  private void output(
      long platformId,
      long eventApiTimestamp,
      CohortMembership cohortMembership,
      Collector<LogUserUser> collector) {
    if (cohortMembership.getPlatformId() != 0L) {
      platformId = cohortMembership.getPlatformId();
    }
    if (cohortMembership.getTiming().getEventApiTimestamp() != 0L) {
      eventApiTimestamp = cohortMembership.getTiming().getEventApiTimestamp();
    }
    output(platformId, eventApiTimestamp, cohortMembership.getUserInfo(), collector);
  }

  private void output(
      long platformId, long eventApiTimestamp, View view, Collector<LogUserUser> collector) {
    if (view.getPlatformId() != 0L) {
      platformId = view.getPlatformId();
    }
    if (view.getTiming().getEventApiTimestamp() != 0L) {
      eventApiTimestamp = view.getTiming().getEventApiTimestamp();
    }
    output(platformId, eventApiTimestamp, view.getUserInfo(), collector);
  }

  private void output(
      long platformId,
      long eventApiTimestamp,
      AutoView autoView,
      Collector<LogUserUser> collector) {
    if (autoView.getPlatformId() != 0L) {
      platformId = autoView.getPlatformId();
    }
    if (autoView.getTiming().getEventApiTimestamp() != 0L) {
      eventApiTimestamp = autoView.getTiming().getEventApiTimestamp();
    }
    output(platformId, eventApiTimestamp, autoView.getUserInfo(), collector);
  }

  private void output(
      long platformId,
      long eventApiTimestamp,
      DeliveryLog deliveryLog,
      Collector<LogUserUser> collector) {
    if (deliveryLog.getPlatformId() != 0L) {
      platformId = deliveryLog.getPlatformId();
    } else if (deliveryLog.getRequest().getPlatformId() != 0L) {
      platformId = deliveryLog.getRequest().getPlatformId();
    }
    if (deliveryLog.getRequest().getTiming().getEventApiTimestamp() != 0L) {
      eventApiTimestamp = deliveryLog.getRequest().getTiming().getEventApiTimestamp();
    }
    output(platformId, eventApiTimestamp, deliveryLog.getRequest().getUserInfo(), collector);
  }

  private void output(
      long platformId,
      long eventApiTimestamp,
      Impression impression,
      Collector<LogUserUser> collector) {
    if (impression.getPlatformId() != 0L) {
      platformId = impression.getPlatformId();
    }
    if (impression.getTiming().getEventApiTimestamp() != 0L) {
      eventApiTimestamp = impression.getTiming().getEventApiTimestamp();
    }
    output(platformId, eventApiTimestamp, impression.getUserInfo(), collector);
  }

  private void output(
      long platformId, long eventApiTimestamp, Action action, Collector<LogUserUser> collector) {
    if (action.getPlatformId() != 0L) {
      platformId = action.getPlatformId();
    }
    if (action.getTiming().getEventApiTimestamp() != 0L) {
      eventApiTimestamp = action.getTiming().getEventApiTimestamp();
    }
    output(platformId, eventApiTimestamp, action.getUserInfo(), collector);
  }

  private void output(
      long platformId,
      long eventApiTimestamp,
      Diagnostics diagnostics,
      Collector<LogUserUser> collector) {
    if (diagnostics.getPlatformId() != 0L) {
      platformId = diagnostics.getPlatformId();
    }
    if (diagnostics.getTiming().getEventApiTimestamp() != 0L) {
      eventApiTimestamp = diagnostics.getTiming().getEventApiTimestamp();
    }
    output(platformId, eventApiTimestamp, diagnostics.getUserInfo(), collector);
  }

  private void output(
      long platformId,
      long eventApiTimestamp,
      UserInfo userInfo,
      Collector<LogUserUser> collector) {
    if (!userInfo.getLogUserId().isEmpty() && !userInfo.getUserId().isEmpty()) {
      collector.collect(
          LogUserUser.newBuilder()
              .setPlatformId(platformId)
              .setEventApiTimestamp(eventApiTimestamp)
              .setUserId(userInfo.getUserId())
              .setLogUserId(userInfo.getLogUserId())
              .build());
    }
  }
}
