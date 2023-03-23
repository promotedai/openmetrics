package ai.promoted.metrics.logprocessor.common.functions.validate;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Impression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValidateImpressionTest extends BaseValidateTest<Impression> {

  private ValidateImpression validate;

  @BeforeEach
  public void setUp() {
    super.setUp();
    validate = new ValidateImpression();
  }

  @Test
  public void valid() throws Exception {
    Impression impression =
        Impression.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
            .setContentId(CONTENT_ID)
            .build();
    validate.processElement(impression, mockContext, mockOut);
    verify(mockContext, never()).output(eq(ValidateView.INVALID_TAG), any(ValidationError.class));
    verify(mockOut).collect(impression);
  }

  @Test
  public void missingLogUserId() throws Exception {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setTiming(getProtoTiming())
            .setViewId(VIEW_ID)
            .setRequestId(REQUEST_ID)
            .setViewId(VIEW_ID)
            .setImpressionId(IMPRESSION_ID)
            .setInsertionId(INSERTION_ID)
            .build();
    validate.processElement(impression, mockContext, mockOut);

    // Future validation tests do not need assert the full message.  It can call createError.
    verify(mockContext)
        .output(
            ValidateUser.INVALID_TAG,
            ValidationError.newBuilder()
                .setRecordType(RecordType.IMPRESSION)
                .setErrorType(ErrorType.MISSING_FIELD)
                .setField(Field.LOG_USER_ID)
                .setPlatformId(PLATFORM_ID)
                .setViewId(VIEW_ID)
                .setRequestId(REQUEST_ID)
                .setImpressionId(IMPRESSION_ID)
                .setResponseInsertionId(INSERTION_ID)
                .setTiming(getAvroTiming())
                .build());
    verify(mockOut, never()).collect(impression);
  }

  @Test
  public void missingJoinableId() throws Exception {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
            .setTiming(getProtoTiming())
            .setViewId(VIEW_ID)
            .setRequestId(REQUEST_ID)
            .setViewId(VIEW_ID)
            .setImpressionId(IMPRESSION_ID)
            .build();
    validate.processElement(impression, mockContext, mockOut);

    // Future validation tests do not need assert the full message.  It can call createError.
    verify(mockContext)
        .output(
            ValidateUser.INVALID_TAG,
            ValidationError.newBuilder()
                .setRecordType(RecordType.IMPRESSION)
                .setErrorType(ErrorType.MISSING_JOINABLE_ID)
                .setField(Field.MULTIPLE)
                .setPlatformId(PLATFORM_ID)
                .setLogUserId(LOG_USER_ID)
                .setViewId(VIEW_ID)
                .setRequestId(REQUEST_ID)
                .setImpressionId(IMPRESSION_ID)
                .setTiming(getAvroTiming())
                .build());
    verify(mockOut, never()).collect(impression);
  }
}
