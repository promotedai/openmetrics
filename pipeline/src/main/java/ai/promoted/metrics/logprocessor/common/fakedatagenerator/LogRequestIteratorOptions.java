package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactoryOptions;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentType;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.FakeInsertionSurface;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Insertion;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import ai.promoted.proto.event.View;
import com.google.auto.value.AutoValue;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/** Options for LogRequestIterator. */
@AutoValue
public abstract class LogRequestIteratorOptions {

    public static ContentDBFactoryOptions DEFAULT_CONTENT_OPTIONS = ContentDBFactoryOptions.builder()
            .setStores(10)
            .setItemsPerStore(10)
            .setPromotionsPerItem(2)
            .build();

    /* The details for the first InsertionSurface. */
    abstract FakeInsertionSurface requestInsertionSurface0();
    abstract Optional<FakeInsertionSurface> requestInsertionSurface1();
    abstract int users();
    abstract int sessionsPerUser();
    abstract int viewsPerSession();
    abstract int autoViewsPerSession();
    abstract int delayBetweenViewInSeconds();
    abstract int delayBetweenAutoViewInSeconds();
    abstract int requestsPerViews();
    abstract int responseInsertionsPerRequest();
    abstract float insertionImpressedRate();
    abstract float impressionNavigateRate();
    abstract float navigateCheckoutRate();
    abstract float navigateAddToCartRate();
    abstract float checkoutPurchaseRate();
    public abstract ContentDBFactoryOptions contentDBFactoryOptions();
    abstract float missingViewRate();
    abstract float missingAutoViewRate();
    abstract float missingDeliveryLogRate();
    abstract float missingImpressionRate();
    // Probability that a DeliveryLog is generated by the SDKs mini-delivery.
    abstract float miniSdkRate();
    // The effective rate is conditioned on miniSdkRate (i.e. this value is Pr(shadowRate | sdkRate)).
    abstract float shadowTrafficRate();
    // Percent of impressions that should have
    abstract float redundantImpressionRate();
    abstract int maxRedundantImpressionsPerDeliveryLog();

    abstract Supplier<Instant> nowSupplier();
    abstract Supplier<Instant> eventApiTimestampSupplier();
    abstract Supplier<String> userUuidSupplier();
    abstract Function<String, String> logUserUuidSupplier();
    abstract Supplier<String> cohortMembershipUuidSupplier();
    abstract Supplier<String> sessionUuidSupplier();
    abstract Supplier<String> viewUuidSupplier();
    abstract Supplier<String> autoViewUuidSupplier();
    abstract Supplier<String> requestUuidSupplier();
    abstract Supplier<String> responseInsertionUuidSupplier();
    abstract Supplier<String> impressionUuidSupplier();
    abstract Supplier<String> actionUuidSupplier();
    abstract Function<Duration, Duration> delayMultiplier();
    // Allows enabling full and matrix separately to populate test data.
    abstract boolean insertionMatrixFormat();
    // Allows enabling full and matrix separately to populate test data.
    abstract boolean insertionFullFormat();
    abstract boolean setupForInferredIds();
    abstract boolean writeUsers();
    abstract boolean writeProductViews();
    abstract Function<SessionProfile.Builder, SessionProfile.Builder> sessionProfileTransform();
    abstract Function<Session.Builder, Session.Builder> sessionTransform();
    abstract Function<View.Builder, View.Builder> viewTransform();
    abstract Function<AutoView.Builder, AutoView.Builder> autoViewTransform();
    abstract Function<DeliveryLog.Builder, DeliveryLog.Builder> deliveryLogTransform();
    abstract Function<Insertion.Builder, Insertion.Builder> responseInsertionTransform();
    abstract Function<Impression.Builder, Impression.Builder> impressionTransform();
    abstract Function<Action.Builder, Action.Builder> navigateTransform();
    abstract Function<Action.Builder, Action.Builder> checkoutTransform();
    abstract Function<Action.Builder, Action.Builder> purchaseTransform();

    public Instant now() {
        return nowSupplier().get();
    }

    public Instant eventApiTimestamp() {
        return eventApiTimestampSupplier().get();
    }

    public String userUuid() {
        return userUuidSupplier().get();
    }

    public String logUserUuid(String userId) {
        return logUserUuidSupplier().apply(userId);
    }

    public String cohortMembershipUuid() {
        return cohortMembershipUuidSupplier().get();
    }

    public String sessionUuid() {
        return sessionUuidSupplier().get();
    }

    public String viewUuid() {
        return viewUuidSupplier().get();
    }

    public String autoViewUuid() {
        return autoViewUuidSupplier().get();
    }

    public String requestUuid() {
        return requestUuidSupplier().get();
    }

    public String responseInsertionUuid() {
        return responseInsertionUuidSupplier().get();
    }

    public String impressionUuid() {
        return impressionUuidSupplier().get();
    }

    public String actionUuid() {
        return actionUuidSupplier().get();
    }

    public static Builder builder() {
        return new AutoValue_LogRequestIteratorOptions.Builder()
            .setRequestInsertionSurface0(FakeInsertionSurface.create(ContentType.ITEM, true, true))
            .setInsertionMatrixFormat(false)
            .setInsertionFullFormat(true)
            .setSessionProfileTransform((x) -> x)
            .setSessionTransform((x) -> x)
            .setViewTransform((x) -> x)
            .setAutoViewTransform((x) -> x)
            .setDeliveryLogTransform((x) -> x)
            .setResponseInsertionTransform((x) -> x)
            .setImpressionTransform((x) -> x)
            .setNavigateTransform((x) -> x)
            .setCheckoutTransform((x) -> x)
            .setPurchaseTransform((x) -> x)
            .setWriteUsers(true)
            .setWriteProductViews(false)
            // OPTIMIZE - Reduce the number of entities that are created for the default builder.
            .setContentDBFactoryOptions(DEFAULT_CONTENT_OPTIONS)
            .setMissingViewRate(0.0f)
            .setMissingAutoViewRate(0.0f)
            .setMissingDeliveryLogRate(0.0f)
            .setMissingImpressionRate(0.0f)
            .setMiniSdkRate(0.5f)
            .setShadowTrafficRate(0.5f)
            .setRedundantImpressionRate(0.0f)
            .setMaxRedundantImpressionsPerDeliveryLog(5);
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setRequestInsertionSurface0(FakeInsertionSurface insertionSurface0);
        public abstract Builder setRequestInsertionSurface1(FakeInsertionSurface insertionSurface1);
        public abstract Builder setRequestInsertionSurface1(Optional<FakeInsertionSurface> insertionSurface1);
        public abstract Builder setUsers(int users);
        public abstract Builder setSessionsPerUser(int sessionsPerUser);
        public abstract Builder setViewsPerSession(int viewsPerSession);
        public abstract Builder setAutoViewsPerSession(int autoViewsPerSession);
        public abstract Builder setDelayBetweenViewInSeconds(int delayBetweenViewInSeconds);
        public abstract Builder setDelayBetweenAutoViewInSeconds(int delayBetweenAutoViewInSeconds);
        public abstract Builder setRequestsPerViews(int requestsPerViews);
        public abstract Builder setResponseInsertionsPerRequest(int responseInsertionsPerRequest);
        public abstract Builder setInsertionImpressedRate(float insertionImpressedRate);
        public abstract Builder setImpressionNavigateRate(float impressionNavigateRate);
        public abstract Builder setNavigateAddToCartRate(float navigateAddToCartRate);
        public abstract Builder setNavigateCheckoutRate(float navigateCheckoutRate);
        public abstract Builder setCheckoutPurchaseRate(float checkoutPurchaseRate);
        public abstract Builder setContentDBFactoryOptions(ContentDBFactoryOptions contentDBFactoryOptions);
        public abstract Builder setMissingViewRate(float missingViewRate);
        public abstract Builder setMissingAutoViewRate(float missingAutoViewRate);
        public abstract Builder setMissingDeliveryLogRate(float missingDeliveryLogRate);
        public abstract Builder setMissingImpressionRate(float missingImpressionRate);
        public abstract Builder setMiniSdkRate(float rate);
        public abstract Builder setShadowTrafficRate(float rate);
        public abstract Builder setRedundantImpressionRate(float rate);
        public abstract Builder setMaxRedundantImpressionsPerDeliveryLog(int maxRedundantImpressionsPerDeliveryLog);
        public abstract Builder setNowSupplier(Supplier<Instant> nowSupplier);
        public abstract Builder setEventApiTimestampSupplier(Supplier<Instant> eventApiTimestampSupplier);
        public abstract Builder setUserUuidSupplier(Supplier<String> userUuidSupplier);
        public abstract Builder setLogUserUuidSupplier(Function<String, String> logUserUuidSupplier);
        public abstract Builder setCohortMembershipUuidSupplier(Supplier<String> cohortMembershipUuidSupplier);
        public abstract Builder setSessionUuidSupplier(Supplier<String> sessionUuidSupplier);
        public abstract Builder setViewUuidSupplier(Supplier<String> viewUuidSupplier);
        public abstract Builder setAutoViewUuidSupplier(Supplier<String> autoViewUuidSupplier);
        public abstract Builder setRequestUuidSupplier(Supplier<String> requestUuidSupplier);
        public abstract Builder setResponseInsertionUuidSupplier(Supplier<String> responseInsertionUuidSupplier);
        public abstract Builder setImpressionUuidSupplier(Supplier<String> impressionUuidSupplier);
        public abstract Builder setActionUuidSupplier(Supplier<String> actionUuidSupplier);
        public abstract Builder setDelayMultiplier(Function<Duration, Duration> delayMultiplier);
        public abstract Builder setInsertionMatrixFormat(boolean insertionMatrixFormat);
        public abstract Builder setInsertionFullFormat(boolean insertionFullFormat);
        public abstract Builder setSetupForInferredIds(boolean setupForInferredIds);
        public abstract Builder setWriteUsers(boolean writeUsers);
        public abstract Builder setWriteProductViews(boolean writeProductViews);
        public abstract Builder setSessionProfileTransform(Function<SessionProfile.Builder, SessionProfile.Builder> sessionProfileTransform);
        public abstract Builder setSessionTransform(Function<Session.Builder, Session.Builder> sessionTransform);
        public abstract Builder setViewTransform(Function<View.Builder, View.Builder> viewTransform);
        public abstract Builder setAutoViewTransform(Function<AutoView.Builder, AutoView.Builder> autoViewTransform);
        public abstract Builder setDeliveryLogTransform(Function<DeliveryLog.Builder, DeliveryLog.Builder> deliveryLogTransform);
        public abstract Builder setResponseInsertionTransform(Function<Insertion.Builder, Insertion.Builder> responseInsertionTransform);
        public abstract Builder setImpressionTransform(Function<Impression.Builder, Impression.Builder> impressionTransform);
        public abstract Builder setNavigateTransform(Function<Action.Builder, Action.Builder> navigateTransform);
        public abstract Builder setCheckoutTransform(Function<Action.Builder, Action.Builder> checkoutTransform);
        public abstract Builder setPurchaseTransform(Function<Action.Builder, Action.Builder> purchaseTransform);
        public abstract LogRequestIteratorOptions build();
    }
}