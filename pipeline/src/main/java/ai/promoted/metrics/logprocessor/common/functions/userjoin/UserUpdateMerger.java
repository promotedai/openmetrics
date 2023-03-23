package ai.promoted.metrics.logprocessor.common.functions.userjoin;

import ai.promoted.proto.common.UserInfo;
import ai.promoted.proto.event.User;
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Merges sorted user updates and outputs the "effective user" at the time set in "advanceTo".
 *
 * <p>UpdatedUserBuilder is used by the UserJoin function to get an effective final User record for
 * a singe LUID. This allows us to do a hacky time lookup. This class helps merging in synthetic
 * User records (userIds on DeliveryLogs) and clean up older state. This does not support undoing
 * the "advanceTo" function.
 *
 * <p>TODO - might make sense to advance more in advanceTo if an upcoming User has properties.
 */
class UserUpdateMerger {
  private final List<User> userUpdates;
  private final boolean runAssert;
  // Can be treated like an exclusive index.
  private int nextUser;
  // Is null at start or before there is a User record.
  @Nullable private User.Builder userBuilder;

  /**
   * @param userUpdates list of user updates sorted by logTimestamp. Not checking here since this
   *     class is package protected and avoid the performance hit.
   */
  UserUpdateMerger(List<User> userUpdates, boolean runAssert) {
    this.userUpdates = userUpdates;
    this.runAssert = runAssert;
    nextUser = 0;
  }

  public void advanceTo(long timestamp) {
    // Attempts to get temporal correctness.  Useful during backfills.
    while (nextUser < userUpdates.size()
        && timestamp >= userUpdates.get(nextUser).getTiming().getLogTimestamp()) {
      if (userBuilder == null) {
        userBuilder = userUpdates.get(nextUser).toBuilder();
      } else {
        // Replace based on top-level fields.  We use this logic to handle the synthetic user case
        // (created from DeliveryLogs).
        User userUpdate = userUpdates.get(nextUser);
        // Do not use proto mergeFrom since we want to replace the top-level fields.
        if (userUpdate.hasUserInfo()) {
          UserInfo updateUserInfo = userUpdate.getUserInfo();
          if (runAssert && userBuilder.hasUserInfo()) {
            if (!userBuilder.getUserInfo().getLogUserId().isEmpty()
                && !updateUserInfo.getLogUserId().isEmpty()
                && !userBuilder
                    .getUserInfo()
                    .getLogUserId()
                    .equals(updateUserInfo.getLogUserId())) {
              throw new IllegalStateException(
                  "Encountered unexpected change in logUserIds; userUpdates=" + userUpdates);
            }
            if (!userBuilder.getUserInfo().getUserId().isEmpty()
                && !updateUserInfo.getUserId().isEmpty()
                && !userBuilder.getUserInfo().getUserId().equals(updateUserInfo.getUserId())) {
              throw new IllegalStateException(
                  "Encountered unexpected change in userIds; userUpdates=" + userUpdates);
            }
          }
          UserInfo.Builder userInfoBuilder = userBuilder.getUserInfoBuilder();
          if (!updateUserInfo.getUserId().isEmpty()) {
            userInfoBuilder.setUserId(updateUserInfo.getUserId());
          }
          if (!updateUserInfo.getLogUserId().isEmpty()) {
            userInfoBuilder.setLogUserId(updateUserInfo.getLogUserId());
          }
          // Once an internalUser, always an internal user.
          if (updateUserInfo.getIsInternalUser()) {
            userInfoBuilder.setIsInternalUser(true);
          }
          // Use the latest ignoreUsage.  Not great since User records can break it.
          userInfoBuilder.setIgnoreUsage(updateUserInfo.getIgnoreUsage());
        }
        // ClientInfo is not useful.  We'll skip for now.
        if (userUpdate.hasTiming()) {
          userBuilder.setTiming(userUpdate.getTiming());
        }
        if (userUpdate.hasProperties()) {
          userBuilder.setProperties(userUpdate.getProperties());
        }
      }
      nextUser++;
    }
  }

  /** Returns null if no effective User. Otherwise, merges the User records. */
  public User getEffectiveUser() {
    if (userBuilder != null) {
      return userBuilder.clone().build();
    } else {
      return null;
    }
  }

  /** Returns the size of getRemainingUserUpdates() without performing the subList call. */
  public int getRemainingUserUpdatesCount() {
    return userUpdates.size() - nextUser;
  }

  /** Returns the user updates that have not been applied to the effective User. */
  public List<User> getRemainingUserUpdates() {
    if (nextUser >= userUpdates.size()) {
      return ImmutableList.of();
    } else {
      return userUpdates.subList(nextUser, userUpdates.size());
    }
  }
}
