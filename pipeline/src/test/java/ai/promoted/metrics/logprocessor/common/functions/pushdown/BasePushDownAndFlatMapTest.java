package ai.promoted.metrics.logprocessor.common.functions.pushdown;

import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class BasePushDownAndFlatMapTest<T> {

  static final long PLATFORM_ID = 1L;
  static final String USER_ID = "userId1";
  static final String UPPERCASE_LOG_USER_ID = "logUserId1";
  static final String LOWERCASE_LOG_USER_ID = "loguserid1";
  static final long TIME_EPOCH_MILLIS = 1601516627000L;

  static final long OTHER_PLATFORM_ID = PLATFORM_ID + 1;
  static final String OTHER_USER_ID = USER_ID + "alt";
  static final String OTHER_LOG_USER_ID = LOWERCASE_LOG_USER_ID + "alt";
  static final long OTHER_TIME_EPOCH_MILLIS = TIME_EPOCH_MILLIS + 1;

  static final String SESSION_ID = "sessionId1";
  static final String VIEW_ID = "viewId1";
  static final String REQUEST_ID = "requestId1";
  static final String INSERTION_ID = "insertionId1";
  static final String IMPRESSION_ID = "impressionId1";
  static final String ACTION_ID = "actionId1";
  static final String SURFACE_NAME = "search";

  static final ClientInfo CLIENT_INFO = ClientInfo.newBuilder()
          .setTrafficType(ClientInfo.TrafficType.PRODUCTION)
          .setClientType(ClientInfo.ClientType.PLATFORM_SERVER)
          .build();

  static Timing createTiming(long timestamp) {
    return Timing.newBuilder()
            .setClientLogTimestamp(timestamp)
            .build();
  }

  static LogRequest.Builder setBaseFields(LogRequest.Builder builder) {
    return builder
            .setPlatformId(PLATFORM_ID)
            .setTiming(createTiming(TIME_EPOCH_MILLIS));
  }

  List<T> collector;

  @BeforeEach
  public void setUp() {
    collector = new ArrayList();
  }
}
