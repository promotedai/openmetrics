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
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValidateDeliveryLogTest extends BaseValidateTest<DeliveryLog> {

  private ValidateDeliveryLog validate;

  @BeforeEach
  public void setUp() {
    super.setUp();
    validate = new ValidateDeliveryLog(true);
  }

  @Test
  public void valid() throws Exception {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setRequest(
                Request.newBuilder().setUserInfo(UserInfo.newBuilder().setAnonUserId("123")))
            .build();
    validate.processElement(deliveryLog, mockContext, mockOut);
    verifyValid(deliveryLog);
  }

  @Test
  public void missingAnonUserId() throws Exception {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setRequest(
                Request.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setTiming(getProtoTiming())
                    .setViewId(VIEW_ID)
                    .setRequestId(REQUEST_ID))
            .build();
    validate.processElement(deliveryLog, mockContext, mockOut);

    // Future validation tests do not need assert the full message.  It can call createError.
    verify(mockContext)
        .output(
            ValidateDeliveryLog.VALIDATION_ERROR_TAG,
            ValidationError.newBuilder()
                .setRecordType(RecordType.DELIVERY_LOG)
                .setErrorType(ErrorType.MISSING_FIELD)
                .setField(Field.ANON_USER_ID)
                .setPlatformId(PLATFORM_ID)
                .setViewId(VIEW_ID)
                .setRequestId(REQUEST_ID)
                .setTiming(getAvroTiming())
                .build());
    verify(mockContext).output(validate.getInvalidRecordTag(), deliveryLog);
    verify(mockOut, never()).collect(deliveryLog);
  }

  @Test
  public void missingAnonUserId_optional() throws Exception {
    validate = new ValidateDeliveryLog(false);
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setPlatformId(PLATFORM_ID)
            .setRequest(
                Request.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setTiming(getProtoTiming())
                    .setViewId(VIEW_ID)
                    .setRequestId(REQUEST_ID))
            .build();
    validate.processElement(deliveryLog, mockContext, mockOut);
    verifyValid(deliveryLog);
  }

  @Test
  public void mismatchedMatrixHeaderLength() throws Exception {
    DeliveryLog deliveryLog =
        DeliveryLog.newBuilder()
            .setRequest(
                Request.newBuilder()
                    .setPlatformId(PLATFORM_ID)
                    .setViewId(VIEW_ID)
                    .setRequestId(REQUEST_ID)
                    .setTiming(getProtoTiming())
                    .setUserInfo(UserInfo.newBuilder().setAnonUserId(ANON_USER_ID))
                    .addAllInsertionMatrixHeaders(Arrays.asList("1", "2"))
                    .setInsertionMatrix(
                        ListValue.newBuilder()
                            .addValues(
                                Value.newBuilder()
                                    .setListValue(
                                        ListValue.newBuilder()
                                            .addAllValues(
                                                Arrays.asList(
                                                    Value.newBuilder()
                                                        .setStringValue("a")
                                                        .build()))))))
            .build();
    validate.processElement(deliveryLog, mockContext, mockOut);

    verify(mockContext)
        .output(
            ValidateDeliveryLog.VALIDATION_ERROR_TAG,
            ValidationError.newBuilder()
                .setRecordType(RecordType.DELIVERY_LOG)
                .setErrorType(ErrorType.MISMATCHED_MATRIX_HEADER_LENGTH)
                .setField(Field.MULTIPLE)
                .setPlatformId(PLATFORM_ID)
                .setViewId(VIEW_ID)
                .setRequestId(REQUEST_ID)
                .setTiming(getAvroTiming())
                .setAnonUserId(ANON_USER_ID)
                .build());
    verify(mockContext).output(validate.getInvalidRecordTag(), deliveryLog);
    verify(mockOut, never()).collect(deliveryLog);
  }

  private void verifyValid(DeliveryLog deliveryLog) {
    verify(mockContext, never())
        .output(eq(ValidateDeliveryLog.VALIDATION_ERROR_TAG), any(ValidationError.class));
    verify(mockContext, never()).output(eq(validate.getInvalidRecordTag()), any(DeliveryLog.class));
    verify(mockOut).collect(deliveryLog);
  }
}
