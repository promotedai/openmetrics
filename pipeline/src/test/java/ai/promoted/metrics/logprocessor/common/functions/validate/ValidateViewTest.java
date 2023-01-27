package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.View;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ValidateViewTest extends BaseValidateTest<View> {

    private ValidateView validate;

    @BeforeEach
    public void setUp() {
        super.setUp();
        validate = new ValidateView();
    }

    @Test
    public void valid() throws Exception {
        View view = View.newBuilder()
            .setUserInfo(UserInfo.newBuilder()
                    .setLogUserId("123"))
            .build();
        validate.processElement(view, mockContext, mockOut);
        verify(mockContext, never()).output(eq(ValidateView.INVALID_TAG), any(ValidationError.class));
        verify(mockOut).collect(view);
    }

    @Test
    public void missingLogUserId() throws Exception {
        View view = View.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setTiming(getProtoTiming())
                .setViewId(VIEW_ID)
                .build();
        validate.processElement(view, mockContext, mockOut);

        // Future validation tests do not need assert the full message.  It can call createError.
        verify(mockContext).output(ValidateUser.INVALID_TAG,
                ValidationError.newBuilder()
                        .setRecordType(RecordType.VIEW)
                        .setErrorType(ErrorType.MISSING_FIELD)
                        .setField(Field.LOG_USER_ID)
                        .setPlatformId(PLATFORM_ID)
                        .setViewId(VIEW_ID)
                        .setTiming(getAvroTiming())
                        .build());
        verify(mockOut, never()).collect(view);
    }
}