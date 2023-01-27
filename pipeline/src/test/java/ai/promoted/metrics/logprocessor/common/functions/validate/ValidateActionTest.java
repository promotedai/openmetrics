package ai.promoted.metrics.logprocessor.common.functions.validate;

import ai.promoted.metrics.common.Field;
import ai.promoted.metrics.common.RecordType;
import ai.promoted.metrics.common.Timing;
import ai.promoted.metrics.error.ErrorType;
import ai.promoted.metrics.error.ValidationError;
import ai.promoted.proto.common.CurrencyCode;
import ai.promoted.proto.common.Money;
import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.Cart;
import ai.promoted.proto.event.CartContent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ValidateActionTest extends BaseValidateTest<Action> {

    private ValidateAction validate;

    @BeforeEach
    public void setUp() {
        super.setUp();
        validate = new ValidateAction();
    }

    @Test
    public void valid() throws Exception {
        Action action = Action.newBuilder()
            .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
            .setContentId(CONTENT_ID)
            .build();
        validate.processElement(action, mockContext, mockOut);
        verifyNoMoreInteractions(mockContext);
        verify(mockOut).collect(action);
    }

    @Test
    public void validWithCart_withActionContentId() throws Exception {
        Action action = Action.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setContentId(CONTENT_ID)
                .setCart(Cart.newBuilder()
                        .addContents(CartContent.newBuilder()
                                .setContentId("a")
                                .setQuantity(1)
                                .setPricePerUnit(Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(1000000))))
                .build();
        validate.processElement(action, mockContext, mockOut);
        verifyNoMoreInteractions(mockContext);
        verify(mockOut).collect(action);
    }

    @Test
    public void validWithCart_withoutActionContentId() throws Exception {
        Action action = Action.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setCart(Cart.newBuilder()
                        .addContents(CartContent.newBuilder()
                                .setContentId("a")
                                .setQuantity(1)
                                .setPricePerUnit(Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(1000000))))
                .build();
        validate.processElement(action, mockContext, mockOut);
        verifyNoMoreInteractions(mockContext);
        verify(mockOut).collect(action);
    }

    @Test
    public void missingLogUserId() throws Exception {
        Action action = Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setTiming(getProtoTiming())
                .setViewId(VIEW_ID)
                .setRequestId(REQUEST_ID)
                .setViewId(VIEW_ID)
                .setImpressionId(IMPRESSION_ID)
                .setActionId(ACTION_ID)
                .build();
        validate.processElement(action, mockContext, mockOut);

        // Future validation tests do not need assert the full message.  It can call createError.
        verify(mockContext).output(ValidateUser.INVALID_TAG,
                ValidationError.newBuilder()
                        .setRecordType(RecordType.ACTION)
                        .setErrorType(ErrorType.MISSING_FIELD)
                        .setField(Field.LOG_USER_ID)
                        .setPlatformId(PLATFORM_ID)
                        .setViewId(VIEW_ID)
                        .setRequestId(REQUEST_ID)
                        .setImpressionId(IMPRESSION_ID)
                        .setActionId(ACTION_ID)
                        .setTiming(getAvroTiming())
                        .build());
        verifyNoMoreInteractions(mockContext);
        verifyNoInteractions(mockOut);
    }

    @Test
    public void missingJoinableId() throws Exception {
        Action action = Action.newBuilder()
                .setPlatformId(PLATFORM_ID)
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setTiming(getProtoTiming())
                .setViewId(VIEW_ID)
                .setRequestId(REQUEST_ID)
                .setViewId(VIEW_ID)
                .setActionId(ACTION_ID)
                .build();
        validate.processElement(action, mockContext, mockOut);

        // Future validation tests do not need assert the full message.  It can call createError.
        verify(mockContext).output(ValidateUser.INVALID_TAG,
                ValidationError.newBuilder()
                        .setRecordType(RecordType.ACTION)
                        .setErrorType(ErrorType.MISSING_JOINABLE_ID)
                        .setField(Field.MULTIPLE)
                        .setPlatformId(PLATFORM_ID)
                        .setLogUserId(LOG_USER_ID)
                        .setViewId(VIEW_ID)
                        .setRequestId(REQUEST_ID)
                        .setActionId(ACTION_ID)
                        .setTiming(getAvroTiming())
                        .build());
        verifyNoMoreInteractions(mockContext);
        verifyNoInteractions(mockOut);
    }

    @Test
    public void missingCartContentQuantity() throws Exception {
        Action action = Action.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setTiming(getProtoTiming())
                .setContentId(CONTENT_ID)
                .setActionId(ACTION_ID)
                .setCart(Cart.newBuilder()
                        .addContents(CartContent.newBuilder()
                                .setContentId("a")
                                .setPricePerUnit(Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(1000000))))
                .build();
        validate.processElement(action, mockContext, mockOut);

        // Future validation tests do not need assert the full message.  It can call createError.
        verify(mockContext).output(ValidateUser.INVALID_TAG,
                ValidationError.newBuilder()
                        .setRecordType(RecordType.ACTION)
                        .setErrorType(ErrorType.MISSING_FIELD)
                        .setField(Field.CART_CONTENT_QUANTITY)
                        .setLogUserId(LOG_USER_ID)
                        .setActionId(ACTION_ID)
                        .setTiming(getAvroTiming())
                        .build());
        verifyNoMoreInteractions(mockContext);
        verifyNoInteractions(mockOut);
    }

    @Test
    public void missingCartContentId() throws Exception {
        Action action = Action.newBuilder()
                .setUserInfo(UserInfo.newBuilder().setLogUserId(LOG_USER_ID))
                .setActionId(ACTION_ID)
                .setContentId(CONTENT_ID)
                .setCart(Cart.newBuilder()
                        .addContents(CartContent.newBuilder()
                                .setQuantity(1)
                                .setPricePerUnit(Money.newBuilder().setCurrencyCode(CurrencyCode.USD).setAmountMicros(1000000))))
                .build();
        validate.processElement(action, mockContext, mockOut);

        verify(mockContext).output(ValidateUser.INVALID_TAG,
                ValidationError.newBuilder()
                        .setRecordType(RecordType.ACTION)
                        .setErrorType(ErrorType.MISSING_FIELD)
                        .setField(Field.CART_CONTENT_ID)
                        .setLogUserId(LOG_USER_ID)
                        .setActionId(ACTION_ID)
                        .setTiming(Timing.newBuilder().build())
                        .build());
        verifyNoMoreInteractions(mockContext);
        verifyNoInteractions(mockOut);
    }
}