package ai.promoted.metrics.logprocessor.job.counter;

import ai.promoted.proto.delivery.internal.features.AggMetric;
import ai.promoted.proto.delivery.internal.features.CountFeatureMask;
import ai.promoted.proto.delivery.internal.features.CountType;
import ai.promoted.proto.delivery.internal.features.CountWindow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.EnumSet;

/** Functions to help deal with computed counter feature ids. */
@VisibleForTesting
interface FeatureId {
    static long itemDeviceCount(AggMetric value, int bucket, String granularity) {
        CountWindow window = getCountWindow(bucket, granularity);
        return featureId(CountType.ITEM_DEVICE_COUNT, value, window);
    }

    static long queryCount(AggMetric value, int bucket, String granularity) {
        CountWindow window = getCountWindow(bucket, granularity);
        return featureId(CountType.QUERY_COUNT, value, window);
    }

    static long itemQueryCount(AggMetric value, int bucket, String granularity) {
        CountWindow window = getCountWindow(bucket, granularity);
        return featureId(CountType.ITEM_QUERY_COUNT, value, window);
    }

    static long userCount(boolean isLogUser, AggMetric value, int bucket, String granularity) {
        CountType countType = isLogUser ? CountType.LOG_USER_COUNT : CountType.USER_COUNT;
        CountWindow window = getCountWindow(bucket, granularity);
        return featureId(countType, value, window);
    }

    static long lastUserContentTimestamp(boolean isLogUser, AggMetric value) {
        CountType countType = isLogUser ? CountType.LOG_USER_ITEM_HOURS_AGO : CountType.USER_ITEM_HOURS_AGO;
        return featureId(countType, value, CountWindow.NONE);
    }

    static long lastUserContentCount(boolean isLogUser, AggMetric value) {
        CountType countType = isLogUser ? CountType.LOG_USER_ITEM_COUNT : CountType.USER_ITEM_COUNT;
        return featureId(countType, value, CountWindow.DAY_90);
    }

    static long lastUserQueryTimestamp(boolean isLogUser, AggMetric value) {
        CountType countType = isLogUser ? CountType.LOG_USER_QUERY_HOURS_AGO : CountType.USER_QUERY_HOURS_AGO;
        return featureId(countType, value, CountWindow.NONE);
    }

    static long lastUserQueryCount(boolean isLogUser, AggMetric value) {
        CountType countType = isLogUser ? CountType.LOG_USER_QUERY_COUNT : CountType.USER_QUERY_COUNT;
        return featureId(countType, value, CountWindow.DAY_90);
    }

    static CountWindow getCountWindow(int bucket, String granularity) {
        switch (granularity) {
            case "h":
                assert bucket == 1;
                return CountWindow.HOUR;
            case "d":
                switch (bucket) {
                    case 1:
                        return CountWindow.DAY;
                    case 7:
                        return CountWindow.DAY_7;
                    case 30:
                        return CountWindow.DAY_30;
                    case 90:
                        return CountWindow.DAY_90;
                    default:
                        throw new IllegalArgumentException("Unexpected bucket: " + bucket);
                }
            default:
                throw new IllegalArgumentException("Unexpected granularity: " + granularity);
        }
    }

    static long featureId(CountType type, AggMetric value, CountWindow window) {
        return (type != null ? type.getNumber() : 0)
            | (value != null ? value.getNumber() : 0)
            | (window != null ? window.getNumber() : 0);
    }

    /** This provides the cross-product of all the given segments. */
    static ImmutableSet<Long> expandFeatureIds(EnumSet<CountType> types, EnumSet<AggMetric> metrics, EnumSet<CountWindow> windows) {
        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        for (CountType t : types) {
            for (AggMetric a : metrics) {
                for (CountWindow w : windows) {
                    builder.add(FeatureId.featureId(t, a, w));
                }
            }
        }
        return builder.build();
    }

    /** This just expands out against the given metrics. */
    static ImmutableSet<Long> expandFeatureIds(Iterable<Long> others, EnumSet<AggMetric> metrics) {
        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        for (long l : others) {
            for (AggMetric a : metrics) {
                builder.add(l | a.getNumber());
            }
        }
        return builder.build();
    }

    static CountType countType(long id) {
        return CountType.forNumber((int) id & CountFeatureMask.TYPE.getNumber());
    }

    static AggMetric aggMetric(long id) {
        return AggMetric.forNumber((int) id & CountFeatureMask.AGG_METRIC.getNumber());
    }

    static CountWindow countWindow(long id) {
        return CountWindow.forNumber((int) id & CountFeatureMask.WINDOW.getNumber());
    }
}
