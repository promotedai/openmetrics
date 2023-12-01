package ai.promoted.metrics.logprocessor.job.join;

import static com.google.common.truth.Truth.assertThat;

import ai.promoted.proto.common.Properties;
import ai.promoted.proto.common.Timing;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.TinyCommonInfo;
import ai.promoted.proto.event.TinyImpression;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;

public class ToTinyImpressionUnitTest {

  @Test
  public void map() {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
            .setRequestId("req1")
            .setViewId("view1")
            .setInsertionId("ins1")
            .setContentId("content1")
            .setImpressionId("imp1")
            .build();
    assertThat(new ToTinyImpression().map(impression))
        .isEqualTo(
            TinyImpression.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUserId1")
                        .setEventApiTimestamp(1L))
                .setViewId("view1")
                .setRequestId("req1")
                .setInsertionId("ins1")
                .setImpressionId("imp1")
                .setContentId("content1")
                .build());
  }

  @Test
  public void map_withOtherContentIds() {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(1L)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId("anonUserId1").build())
            .setTiming(Timing.newBuilder().setEventApiTimestamp(1))
            .setRequestId("req1")
            .setViewId("view1")
            .setInsertionId("ins1")
            .setContentId("content1")
            .setImpressionId("imp1")
            .setProperties(
                Properties.newBuilder()
                    .setStruct(
                        Struct.newBuilder()
                            .putFields(
                                "store", Value.newBuilder().setStringValue("store1").build())))
            .build();
    assertThat(new ToTinyImpression().map(impression))
        .isEqualTo(
            TinyImpression.newBuilder()
                .setCommon(
                    TinyCommonInfo.newBuilder()
                        .setPlatformId(1L)
                        .setAnonUserId("anonUserId1")
                        .setEventApiTimestamp(1L))
                .setViewId("view1")
                .setRequestId("req1")
                .setInsertionId("ins1")
                .setContentId("content1")
                .setImpressionId("imp1")
                .build());
  }
}
