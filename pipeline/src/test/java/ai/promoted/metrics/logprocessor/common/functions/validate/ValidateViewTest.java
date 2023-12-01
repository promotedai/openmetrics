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
import ai.promoted.proto.event.View;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValidateViewTest extends BaseValidateTest<View> {

  private ValidateView validate;

  @BeforeEach
  public void setUp() {
    super.setUp();
    validate = new ValidateView(true);
  }

  @Test
  public void valid() throws Exception {
    View view = View.newBuilder().setUserInfo(UserInfo.newBuilder().setAnonUserId("123")).build();
    validate.processElement(view, mockContext, mockOut);
    verifyValid(view);
  }

  @Test
  public void missingAnonUserId() throws Exception {
    View view =
        View.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setTiming(getProtoTiming())
            .setViewId(VIEW_ID)
            .build();
    validate.processElement(view, mockContext, mockOut);

    // Future validation tests do not need assert the full message.  It can call createError.
    verify(mockContext)
        .output(
            ValidateView.VALIDATION_ERROR_TAG,
            ValidationError.newBuilder()
                .setRecordType(RecordType.VIEW)
                .setErrorType(ErrorType.MISSING_FIELD)
                .setField(Field.ANON_USER_ID)
                .setPlatformId(PLATFORM_ID)
                .setViewId(VIEW_ID)
                .setTiming(getAvroTiming())
                .build());
    verify(mockContext).output(validate.getInvalidRecordTag(), view);
    verify(mockOut, never()).collect(view);
  }

  @Test
  public void missingAnonUserId_optional() throws Exception {
    validate = new ValidateView(false);
    View view =
        View.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setTiming(getProtoTiming())
            .setViewId(VIEW_ID)
            .build();
    validate.processElement(view, mockContext, mockOut);
    verifyValid(view);
  }

  private void verifyValid(View view) {
    verify(mockContext, never())
        .output(eq(ValidateView.VALIDATION_ERROR_TAG), any(ValidationError.class));
    verify(mockContext, never()).output(eq(validate.getInvalidRecordTag()), any(View.class));
    verify(mockOut).collect(view);
  }
}
