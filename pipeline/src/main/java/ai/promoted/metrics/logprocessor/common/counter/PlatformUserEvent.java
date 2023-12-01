package ai.promoted.metrics.logprocessor.common.counter;

/** platformId, userId, aggMetric */
public class PlatformUserEvent extends PlatformLogUserEvent {
  public static final String NAME = "plat-usr_$evt";
  public static final String DEPRECATED_NAME = "user";

  public PlatformUserEvent() {}

  public PlatformUserEvent(Long platformId, String user, String aggMetric) {
    super(platformId, user, aggMetric);
  }

  @Override
  protected boolean isLogUser() {
    return false;
  }
}
