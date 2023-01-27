package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ValidateUserTest extends BaseValidateTest<User> {

    private ValidateUser validate;

    @BeforeEach
    public void setUp() {
        super.setUp();
        validate = new ValidateUser();
    }

    @Test
    public void valid() throws Exception {
        User user = User.newBuilder()
            .setUserInfo(UserInfo.newBuilder()
                    .setUserId("123"))
            .build();
        validate.processElement(user, mockContext, mockOut);
        verify(mockContext, never()).output(eq(ValidateUser.INVALID_TAG), any(ValidationError.class));
        verify(mockOut).collect(user);
    }

    @Test
    public void missingUserId() throws Exception {
        User user = User.newBuilder()
                .setUserInfo(UserInfo.newBuilder()
                        .setLogUserId(LOG_USER_ID))
                .setPlatformId(PLATFORM_ID)
                .setTiming(getProtoTiming())
                .build();
        validate.processElement(user, mockContext, mockOut);

        // Future validation tests do not need assert the full message.  It can call createError.
        verify(mockContext).output(ValidateUser.INVALID_TAG,
                ValidationError.newBuilder()
                        .setRecordType(RecordType.USER)
                        .setErrorType(ErrorType.MISSING_FIELD)
                        .setField(Field.USER_ID)
                        .setPlatformId(PLATFORM_ID)
                        .setLogUserId(LOG_USER_ID)
                        .setTiming(getAvroTiming())
                        .build());
        verify(mockOut, never()).collect(user);
    }
}