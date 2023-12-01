package ai.promoted.metrics.logprocessor.common.fakedatagenerator;

import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentDBFactoryOptions;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.ContentType;
import ai.promoted.metrics.logprocessor.common.fakedatagenerator.content.FakeInsertionSurface;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapAction;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapAutoView;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapCohortMembership;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapDeliveryLog;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapDiagnostics;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapImpression;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapUser;
import ai.promoted.metrics.logprocessor.common.functions.pushdown.PushDownAndFlatMapView;
import ai.promoted.proto.common.Browser;
import ai.promoted.proto.common.ClientHintBrand;
import ai.promoted.proto.common.ClientHints;
import ai.promoted.proto.common.Device;
import ai.promoted.proto.common.DeviceType;
import ai.promoted.proto.common.Location;
import ai.promoted.proto.common.Properties;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.delivery.Request;
import ai.promoted.proto.delivery.UseCase;
import ai.promoted.proto.event.Action;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.CohortMembership;
import ai.promoted.proto.event.Diagnostics;
import ai.promoted.proto.event.Impression;
import ai.promoted.proto.event.LogRequest;
import ai.promoted.proto.event.Session;
import ai.promoted.proto.event.SessionProfile;
import ai.promoted.proto.event.User;
import ai.promoted.proto.event.View;
import ai.promoted.proto.event.WebPageView;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Factory for generating test LogRequests. */
public class LogRequestFactory {

  public static List<LogRequest> createFullLogRequests(long timeMillis, int n) {
    return createLogRequests(timeMillis, DetailLevel.FULL, n);
  }

  public static List<LogRequest> createLogRequests(
      long timeMillis, DetailLevel detailLevel, int count) {
    return createLogRequests(createLogRequestOptionsBuilder(timeMillis, detailLevel, count));
  }

  public static List<LogRequest> createLogRequests(
      LogRequestIteratorOptions.Builder optionsBuilder) {
    LogRequestIterator iterator = new LogRequestIterator(optionsBuilder);
    return ImmutableList.copyOf(iterator);
  }

  public static LogRequestIteratorOptions.Builder createLogRequestOptionsBuilder(
      long timeMillis, int count) {
    return createLogRequestOptionsBuilder(timeMillis, DetailLevel.FULL, count);
  }

  public static LogRequestIteratorOptions.Builder createLogRequestOptionsBuilder(
      long timeMillis, DetailLevel detailLevel, int count) {
    return createLogRequestOptionsBuilder(timeMillis, detailLevel, count, "0000", "0000");
  }

  public static LogRequestIteratorOptions.Builder createLogRequestOptionsBuilder(
      long timeMillis,
      DetailLevel detailLevel,
      int count,
      String anonUserIdComponent,
      String eventIdComponent) {
    LogRequestIteratorOptions.Builder builder =
        setDeterministicUuidsAndCounts(
            LogRequestIteratorOptions.builder(),
            timeMillis,
            count,
            anonUserIdComponent,
            eventIdComponent);
    switch (detailLevel) {
      case PARTIAL:
        return builder;
      case FULL:
        return setTestMarketplaceFields(builder);
      default:
        throw new IllegalArgumentException("Unsupported detailLevel=" + detailLevel);
    }
  }

  public static LogRequestIteratorOptions.Builder createStoreToItemBuilder() {
    long timeMillis = 1601596151000L;
    return LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, DetailLevel.PARTIAL, 1)
        .setSetupForInferredIds(true)
        .setMiniSdkRate(0.0f)
        .setShadowTrafficRate(0.0f)
        .setImpressionNavigateRate(1.0f)
        .setNavigateCheckoutRate(1.0f)
        .setNavigateAddToCartRate(1.0f)
        .setCheckoutPurchaseRate(1.0f)
        .setRequestInsertionSurface0(FakeInsertionSurface.create(ContentType.STORE, true, true))
        .setRequestInsertionSurface1(
            Optional.of(FakeInsertionSurface.create(ContentType.ITEM, false, false)));
  }

  public static LogRequestIteratorOptions.Builder createItemShoppingCartBuilder() {
    long timeMillis = 1601596151000L;
    return LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, DetailLevel.PARTIAL, 1)
        .setSetupForInferredIds(false)
        .setUsers(1)
        .setSessionsPerUser(1)
        .setViewsPerSession(1)
        .setAutoViewsPerSession(1)
        .setRequestsPerViews(1)
        .setResponseInsertionsPerRequest(4)
        .setMiniSdkRate(0.0f)
        .setShadowTrafficRate(0.0f)
        .setImpressionNavigateRate(1.0f)
        // Currently need 0.25f so at least 1 insertion has an event.
        .setNavigateCheckoutRate(0.25f)
        // Currently need 0.25f so at least 1 insertion has an event.
        .setNavigateAddToCartRate(0.25f)
        .setCheckoutPurchaseRate(1.0f)
        // TODO - weird things happen if we don't log impression and actions on these surfaces.
        .setRequestInsertionSurface0(FakeInsertionSurface.create(ContentType.STORE, false, false))
        .setRequestInsertionSurface1(
            Optional.of(FakeInsertionSurface.create(ContentType.ITEM, true, true)));
  }

  public static LogRequestIteratorOptions.Builder createStoreInsertionItemShoppingCartBuilder() {
    long timeMillis = 1601596151000L;
    return LogRequestFactory.createLogRequestOptionsBuilder(timeMillis, DetailLevel.PARTIAL, 1)
        .setSetupForInferredIds(false)
        .setUsers(1)
        .setSessionsPerUser(1)
        .setViewsPerSession(1)
        .setAutoViewsPerSession(1)
        .setRequestsPerViews(1)
        .setResponseInsertionsPerRequest(4)
        .setMiniSdkRate(0.0f)
        .setShadowTrafficRate(0.0f)
        .setImpressionNavigateRate(1.0f)
        // Currently need 0.25f so at least 1 insertion has an event.
        .setNavigateCheckoutRate(0.25f)
        // Currently need 0.25f so at least 1 insertion has an event.
        .setNavigateAddToCartRate(0.25f)
        .setCheckoutPurchaseRate(1.0f)
        // TODO - weird things happen if we don't log impression and actions on these surfaces.
        .setRequestInsertionSurface0(FakeInsertionSurface.create(ContentType.STORE, true, true))
        .setRequestInsertionSurface1(
            Optional.of(FakeInsertionSurface.create(ContentType.ITEM, false, false)));
  }

  /**
   * @param anonUserIdComponent allows for inserting 4 characters into the anonUserId UUID.
   * @param eventIdComponent allows for inserting 4 characters into other event UUIDs. Can be used
   *     to change events for the same user.
   */
  private static LogRequestIteratorOptions.Builder setDeterministicUuidsAndCounts(
      LogRequestIteratorOptions.Builder builder,
      long timeMillis,
      int n,
      String anonUserIdComponent,
      String eventIdComponent) {
    IncrementingUUIDSupplier anonUserIdSupplier =
        new IncrementingUUIDSupplier(
            String.format("00000000-0000-0000-%s-000000000000", anonUserIdComponent));
    // Set navigateRate so there's a large chance we have at least one navigate.
    int numImpressions = n * n * n * n * n;
    float navigateRate = Math.max(1.0f / numImpressions, 0.1f);
    return builder
        .setUsers(n)
        .setSessionsPerUser(n)
        .setViewsPerSession(n)
        .setAutoViewsPerSession(n)
        .setDelayBetweenViewInSeconds(1)
        .setDelayBetweenAutoViewInSeconds(1)
        .setRequestsPerViews(n)
        .setResponseInsertionsPerRequest(n)
        .setInsertionImpressedRate(1.0f)
        .setImpressionNavigateRate(navigateRate)
        // TODO - enable.  Might mean updating other tests.
        .setNavigateCheckoutRate(0.0f)
        .setNavigateAddToCartRate(0.0f)
        .setCheckoutPurchaseRate(0.0f)
        .setSetupForInferredIds(false)
        .setShadowTrafficRate(0.0f)
        .setNowSupplier(() -> Instant.ofEpochMilli(timeMillis))
        .setEventApiTimestampSupplier(() -> Instant.ofEpochMilli(timeMillis))
        .setUserUuidSupplier(new StringPrefixUUIDSupplier("userId"))
        .setAnonUserUuidSupplier((ignored) -> anonUserIdSupplier.get())
        .setCohortMembershipUuidSupplier(
            new IncrementingUUIDSupplier(
                String.format("CCCCCCCC-CCCC-CCCC-%s-000000000000", eventIdComponent)))
        .setSessionUuidSupplier(
            new IncrementingUUIDSupplier(
                String.format("11111111-1111-1111-%s-000000000000", eventIdComponent)))
        .setViewUuidSupplier(
            new IncrementingUUIDSupplier(
                String.format("22222222-2222-2222-%s-000000000000", eventIdComponent)))
        .setAutoViewUuidSupplier(
            new IncrementingUUIDSupplier(
                String.format("77777777-7777-7777-%s-000000000000", eventIdComponent)))
        .setRequestUuidSupplier(
            new IncrementingUUIDSupplier(
                String.format("33333333-3333-3333-%s-000000000000", eventIdComponent)))
        .setResponseInsertionUuidSupplier(
            new IncrementingUUIDSupplier(
                String.format("44444444-4444-4444-%s-000000000000", eventIdComponent)))
        .setImpressionUuidSupplier(
            new IncrementingUUIDSupplier(
                String.format("55555555-5555-5555-%s-000000000000", eventIdComponent)))
        .setActionUuidSupplier(
            new IncrementingUUIDSupplier(
                String.format("66666666-6666-6666-%s-000000000000", eventIdComponent)))
        .setContentDBFactoryOptions(
            ContentDBFactoryOptions.builder()
                .setStores(20)
                .setItemsPerStore(20)
                .setPromotionsPerItem(4)
                .build())
        .setDelayMultiplier((delay) -> delay);
  }

  public static LogRequestIteratorOptions.Builder setTestMarketplaceFields(
      LogRequestIteratorOptions.Builder baseBuilder) {
    return baseBuilder
        .setSessionProfileTransform(LogRequestFactory::setTestSessionProfileDetails)
        .setSessionTransform(LogRequestFactory::setTestSessionDetails)
        .setViewTransform(LogRequestFactory::setTestViewDetails)
        .setAutoViewTransform(LogRequestFactory::setTestAutoViewDetails)
        .setDeliveryLogTransform(LogRequestFactory::setTestDeliveryLogDetails);
  }

  private static SessionProfile.Builder setTestSessionProfileDetails(
      SessionProfile.Builder builder) {
    int hash = builder.getSessionId().hashCode();
    return builder.setLocation(
        Location.newBuilder()
            .setLatitude(roundToThousands(-90 + 180 * (5 * hash / ((double) Integer.MAX_VALUE))))
            .setLongitude(
                roundToThousands(-180 + 360 * (7 * hash / ((double) Integer.MAX_VALUE)))));
  }

  private static Session.Builder setTestSessionDetails(Session.Builder builder) {
    return builder.setStartEpochMillis(1601581751000L).setExclusiveEndEpochMillis(1601524151000L);
  }

  private static View.Builder setTestViewDetails(View.Builder builder) {
    return builder
        .setName("Search")
        .setUseCase(UseCase.SEARCH)
        .setSearchQuery("shoes")
        .setViewType(View.ViewType.WEB_PAGE)
        .setDevice(
            Device.newBuilder()
                .setDeviceType(DeviceType.MOBILE)
                .setBrand("Pixel")
                .setIdentifier("long_model_name"))
        .setWebPageView(
            WebPageView.newBuilder()
                .setUrl("https://www.mymarket.com/search?q=shoes")
                .setReferrer("https://www.google.com/search?q=shoes")
                .setBrowser(
                    Browser.newBuilder()
                        .setUserAgent(
                            "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36")
                        .setClientHints(
                            ClientHints.newBuilder()
                                .setIsMobile(false)
                                .addBrand(
                                    ClientHintBrand.newBuilder()
                                        .setBrand("Chromium")
                                        .setVersion("88"))
                                .addBrand(
                                    ClientHintBrand.newBuilder()
                                        .setBrand("Google Chrome")
                                        .setVersion("88"))
                                .addBrand(
                                    ClientHintBrand.newBuilder()
                                        .setBrand(";Not A Brand")
                                        .setVersion("99")))));
  }

  private static AutoView.Builder setTestAutoViewDetails(AutoView.Builder builder) {
    return builder
        .setName("Search")
        .setUseCase(UseCase.SEARCH)
        .setWebPageView(
            WebPageView.newBuilder()
                .setUrl("https://www.mymarket.com/search?shoes")
                .setReferrer("https://www.google.com/search?q=shoes")
                .setBrowser(
                    Browser.newBuilder()
                        .setUserAgent(
                            "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36")
                        .setClientHints(
                            ClientHints.newBuilder()
                                .setIsMobile(false)
                                .addBrand(
                                    ClientHintBrand.newBuilder()
                                        .setBrand("Chromium")
                                        .setVersion("88"))
                                .addBrand(
                                    ClientHintBrand.newBuilder()
                                        .setBrand("Google Chrome")
                                        .setVersion("88"))
                                .addBrand(
                                    ClientHintBrand.newBuilder()
                                        .setBrand(";Not A Brand")
                                        .setVersion("99")))));
  }

  private static DeliveryLog.Builder setTestDeliveryLogDetails(DeliveryLog.Builder builder) {
    setTestRequestDetails(builder.getRequestBuilder());
    return builder;
  }

  private static Request.Builder setTestRequestDetails(Request.Builder builder) {
    // get arbitrary number based on RequestId
    // TODO - add struct bytes one.
    return builder
        .setUseCase(UseCase.SEARCH)
        .setSearchQuery("prom dresses")
        .setDevice(
            Device.newBuilder()
                .setBrowser(
                    Browser.newBuilder()
                        .setUserAgent(
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")))
        .setProperties(
            Properties.newBuilder()
                .setStruct(
                    Struct.newBuilder()
                        .putFields("maxPriceFilter", Value.newBuilder().setNumberValue(100).build())
                        .putFields("maxSizeFilter", Value.newBuilder().setNumberValue(8).build())
                        .putFields(
                            "stylesFilter",
                            Value.newBuilder()
                                .setListValue(
                                    ListValue.newBuilder()
                                        .addValues(
                                            Value.newBuilder().setStringValue("High tops").build())
                                        .build())
                                .build())));
  }

  public static List<User> pushDownToUsers(Iterable<LogRequest> logRequests) {
    List<User> collector = new ArrayList();
    PushDownAndFlatMapUser flatMap = new PushDownAndFlatMapUser();
    logRequests.forEach(logRequest -> flatMap.flatMap(logRequest, collector::add));
    return collector;
  }

  public static List<CohortMembership> pushDownToCohortMemberships(
      Iterable<LogRequest> logRequests) {
    List<CohortMembership> collector = new ArrayList();
    PushDownAndFlatMapCohortMembership flatMap = new PushDownAndFlatMapCohortMembership();
    logRequests.forEach(logRequest -> flatMap.flatMap(logRequest, collector::add));
    return collector;
  }

  public static List<View> pushDownToViews(Iterable<LogRequest> logRequests) {
    List<View> collector = new ArrayList();
    PushDownAndFlatMapView flatMap = new PushDownAndFlatMapView();
    logRequests.forEach(logRequest -> flatMap.flatMap(logRequest, collector::add));
    return collector;
  }

  public static List<AutoView> pushDownToAutoViews(Iterable<LogRequest> logRequests) {
    List<AutoView> collector = new ArrayList();
    PushDownAndFlatMapAutoView flatMap = new PushDownAndFlatMapAutoView();
    logRequests.forEach(logRequest -> flatMap.flatMap(logRequest, collector::add));
    return collector;
  }

  public static List<DeliveryLog> pushDownToDeliveryLogs(Iterable<LogRequest> logRequests) {
    List<DeliveryLog> collector = new ArrayList();
    PushDownAndFlatMapDeliveryLog flatMap = new PushDownAndFlatMapDeliveryLog();
    logRequests.forEach(logRequest -> flatMap.flatMap(logRequest, collector::add));
    return collector;
  }

  public static List<Impression> pushDownToImpressions(Iterable<LogRequest> logRequests) {
    List<Impression> collector = new ArrayList();
    PushDownAndFlatMapImpression flatMap = new PushDownAndFlatMapImpression();
    logRequests.forEach(logRequest -> flatMap.flatMap(logRequest, collector::add));
    return collector;
  }

  public static List<Action> pushDownToActions(Iterable<LogRequest> logRequests) {
    List<Action> collector = new ArrayList();
    PushDownAndFlatMapAction flatMap = new PushDownAndFlatMapAction();
    logRequests.forEach(logRequest -> flatMap.flatMap(logRequest, collector::add));
    return collector;
  }

  public static List<Diagnostics> pushDownToDiagnostics(Iterable<LogRequest> logRequests) {
    List<Diagnostics> collector = new ArrayList();
    PushDownAndFlatMapDiagnostics flatMap = new PushDownAndFlatMapDiagnostics();
    logRequests.forEach(logRequest -> flatMap.flatMap(logRequest, collector::add));
    return collector;
  }

  /* Converter to simplify Stream interactions */
  private static <L, R> Function<L, Stream<R>> toStream(Function<L, List<R>> toList) {
    return value -> toList.apply(value).stream();
  }

  /* Used to get a list of a specific log record from LogRequest. */
  public static <R> List<R> toChildRecords(
      List<LogRequest> logRequests, Function<LogRequest, List<R>> toList) {
    return logRequests.stream().flatMap(toStream(toList)).collect(Collectors.toList());
  }

  private static double roundToThousands(double num) {
    return Math.round(1000.0 * num) / 1000.0;
  }

  /** Specifies type of test data to use. */
  public enum DetailLevel {
    // Does not contain all fields.
    PARTIAL,
    // Contains more optional fields with fake data.  This does not impact the IDs that are used.
    // This adds things like fake request properties and device type.  It's not critical for the
    // join.
    // If you want to remove foreign keys, change the `LogRequestIteratorOptions`.
    FULL,
  }
}
