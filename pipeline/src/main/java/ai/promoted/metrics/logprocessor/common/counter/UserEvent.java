package ai.promoted.metrics.logprocessor.common.counter;

/** platformId, userId, aggMetric */
public class UserEvent extends LogUserEvent {
  public static final String NAME = "user";

  public UserEvent() {}

  public UserEvent(Long platformId, String user, String aggMetric) {
    super(platformId, user, aggMetric);
  }

  @Override
  protected boolean isLogUser() {
    return false;
  }
}
