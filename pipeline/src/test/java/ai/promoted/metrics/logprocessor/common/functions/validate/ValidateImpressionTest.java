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
    validate = new ValidateImpression(true);
  }

  @Test
  public void valid() throws Exception {
    Impression impression =
        Impression.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setAnonUserId(ANON_USER_ID))
            .setContentId(CONTENT_ID)
            .build();
    validate.processElement(impression, mockContext, mockOut);
    verifyValid(impression);
  }

  @Test
  public void missingAnonUserId() throws Exception {
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
            ValidateImpression.VALIDATION_ERROR_TAG,
            ValidationError.newBuilder()
                .setRecordType(RecordType.IMPRESSION)
                .setErrorType(ErrorType.MISSING_FIELD)
                .setField(Field.ANON_USER_ID)
                .setPlatformId(PLATFORM_ID)
                .setViewId(VIEW_ID)
                .setRequestId(REQUEST_ID)
                .setImpressionId(IMPRESSION_ID)
                .setResponseInsertionId(INSERTION_ID)
                .setTiming(getAvroTiming())
                .build());
    verify(mockContext).output(validate.getInvalidRecordTag(), impression);
    verify(mockOut, never()).collect(impression);
  }

  @Test
  public void missingAnonUserId_optional() throws Exception {
    validate = new ValidateImpression(false);
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
    verifyValid(impression);
  }

  @Test
  public void missingJoinableId() throws Exception {
    Impression impression =
        Impression.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setUserInfo(UserInfo.newBuilder().setAnonUserId(ANON_USER_ID))
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
            ValidateImpression.VALIDATION_ERROR_TAG,
            ValidationError.newBuilder()
                .setRecordType(RecordType.IMPRESSION)
                .setErrorType(ErrorType.MISSING_JOINABLE_ID)
                .setField(Field.MULTIPLE)
                .setPlatformId(PLATFORM_ID)
                .setAnonUserId(ANON_USER_ID)
                .setViewId(VIEW_ID)
                .setRequestId(REQUEST_ID)
                .setImpressionId(IMPRESSION_ID)
                .setTiming(getAvroTiming())
                .build());
    verify(mockContext).output(validate.getInvalidRecordTag(), impression);
    verify(mockOut, never()).collect(impression);
  }

  private void verifyValid(Impression impression) {
    verify(mockContext, never())
        .output(eq(ValidateImpression.VALIDATION_ERROR_TAG), any(ValidationError.class));
    verify(mockContext, never()).output(eq(validate.getInvalidRecordTag()), any(Impression.class));
    verify(mockOut).collect(impression);
  }
}
