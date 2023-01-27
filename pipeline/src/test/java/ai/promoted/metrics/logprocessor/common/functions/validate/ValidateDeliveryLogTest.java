package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ValidateDeliveryLogTest extends BaseValidateTest<DeliveryLog> {

    private ValidateDeliveryLog validate;

    @BeforeEach
    public void setUp() {
        super.setUp();
        validate = new ValidateDeliveryLog();
    }

    @Test
    public void valid() throws Exception {
        DeliveryLog deliveryLog = DeliveryLog.newBuilder()
            .setRequest(Request.newBuilder()
                .setUserInfo(UserInfo.newBuilder()
                        .setLogUserId("123")))
            .build();
        validate.processElement(deliveryLog, mockContext, mockOut);
        verify(mockContext, never()).output(eq(ValidateView.INVALID_TAG), any(ValidationError.class));
        verify(mockOut).collect(deliveryLog);
    }

    @Test
    public void missingLogUserId() throws Exception {
        DeliveryLog deliveryLog = DeliveryLog.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setRequest(Request.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setTiming(getProtoTiming())
                        .setViewId(VIEW_ID)
                        .setRequestId(REQUEST_ID))
                .build();
        validate.processElement(deliveryLog, mockContext, mockOut);

        // Future validation tests do not need assert the full message.  It can call createError.
        verify(mockContext).output(ValidateUser.INVALID_TAG,
                ValidationError.newBuilder()
                        .setRecordType(RecordType.DELIVERY_LOG)
                        .setErrorType(ErrorType.MISSING_FIELD)
                        .setField(Field.LOG_USER_ID)
                        .setPlatformId(PLATFORM_ID)
                        .setViewId(VIEW_ID)
                        .setRequestId(REQUEST_ID)
                        .setTiming(getAvroTiming())
                        .build());
        verify(mockOut, never()).collect(deliveryLog);
    }

    @Test
    public void mismatchedMatrixHeaderLength() throws Exception {
        DeliveryLog deliveryLog = DeliveryLog.newBuilder()
                .setRequest(Request.newBuilder()
                        .setPlatformId(PLATFORM_ID)
                        .setViewId(VIEW_ID)
                        .setRequestId(REQUEST_ID)
                        .setTiming(getProtoTiming())
                        .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                        .addAllInsertionMatrixHeaders(Arrays.asList("1", "2"))
                        .setInsertionMatrix(ListValue.newBuilder().addValues(
                                Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(Arrays.asList(
                                        Value.newBuilder().setStringValue("a").build()))))))
                .build();
        validate.processElement(deliveryLog, mockContext, mockOut);

        verify(mockContext).output(ValidateUser.INVALID_TAG,
                ValidationError.newBuilder()
                        .setRecordType(RecordType.DELIVERY_LOG)
                        .setErrorType(ErrorType.MISMATCHED_MATRIX_HEADER_LENGTH)
                        .setField(Field.MULTIPLE)
                        .setPlatformId(PLATFORM_ID)
                        .setViewId(VIEW_ID)
                        .setRequestId(REQUEST_ID)
                        .setTiming(getAvroTiming())
                        .setLogUserId(LOG_USER_ID)
                        .build());
        verify(mockOut, never()).collect(deliveryLog);
    }
}