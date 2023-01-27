package ai.promoted.metrics.logprocessor.common.functions;

import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.proto.common.ClientInfo;
import ai.promoted.proto.delivery.DeliveryLog;
import ai.promoted.proto.event.CombinedDeliveryLog;
import ai.promoted.proto.event.CombinedDeliveryLogOrBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static ai.promoted.metrics.logprocessor.common.util.DeliveryLogUtil.getTrafficPriority;

/**
 * Combines API and SDK DeliveryLogs so we can have both info on our flat records.
 *
 * Keys by Tuple2<platformId, logUserId> in case we can optimize Flink later to not re-shuffle keys across operations.
 * This also keeps our keys consistent with other operation.
 */
public class CombineDeliveryLog extends KeyedProcessFunction<Tuple2<Long, String>, DeliveryLog, CombinedDeliveryLog> {
    private static final Logger LOGGER = LogManager.getLogger(CombineDeliveryLog.class);

    // The sliding window we use to check for the same clientRequestId.
    private final Duration windowDuration;
    private final DebugIds debugIds;
    // There might be edge cases if clients reuse clientRequestId outside of the combine window.
    private MapState<String, CombinedDeliveryLog> clientRequestIdToCombinedDeliveryLog;
    private MapState<Long, List<String>> timerToClientRequestId;

    public CombineDeliveryLog(Duration windowDuration, DebugIds debugIds) {
        this.windowDuration = windowDuration;
        this.debugIds = debugIds;
    }

    @Override
    public void processElement(DeliveryLog input, Context ctx, Collector<CombinedDeliveryLog> collector) throws Exception {
        if (debugIds.matches(input)) {
            LOGGER.info("CombineDeliveryLog processElement ts={}\nwatermark={}\ndeliveryLogInput={}",
                    ctx.timestamp(), ctx.timerService().currentWatermark(), input);
        }

        String clientRequestId = input.getRequest().getClientRequestId();
        // If no clientRequestId, don't try to combine.  Just send through.
        if (clientRequestId.isEmpty()) {
            CombinedDeliveryLog combinedDeliveryLog = mergeDeliveryLog(CombinedDeliveryLog.newBuilder(), input).build();
            if (debugIds.matches(combinedDeliveryLog)) {
                LOGGER.info("CombineDeliveryLog collect ts={}\nwatermark={}\ncombinedDeliveryLog={}",
                        ctx.timestamp(), ctx.timerService().currentWatermark(), combinedDeliveryLog);
            }
            collector.collect(combinedDeliveryLog);
            return;
        }

        CombinedDeliveryLog combinedDeliveryLog = clientRequestIdToCombinedDeliveryLog.get(clientRequestId);
        if (combinedDeliveryLog == null) {
            combinedDeliveryLog = CombinedDeliveryLog.getDefaultInstance();
            // Add the clientRequestId to the timer map.
            Long futureTime = ctx.timestamp() + windowDuration.toMillis();
            List<String> timerClientRequestIds = timerToClientRequestId.get(futureTime);
            if (timerClientRequestIds == null) {
                timerClientRequestIds = new ArrayList<>();
                // TODO - remove try-catch block after finding bad key issue.
                try {
                    ctx.timerService().registerEventTimeTimer(futureTime);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            "Issue with registerEventTimeTimer for key=" + ctx.getCurrentKey() + ", input=" + input,
                            e);
                }
            }
            timerClientRequestIds.add(clientRequestId);
            timerToClientRequestId.put(futureTime, timerClientRequestIds);
        }
        combinedDeliveryLog = mergeDeliveryLog(combinedDeliveryLog.toBuilder(), input).build();
        clientRequestIdToCombinedDeliveryLog.put(clientRequestId, combinedDeliveryLog);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CombinedDeliveryLog> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        List<String> clientRequestIds = timerToClientRequestId.get(timestamp);
        for (String clientRequestId : clientRequestIds) {
            CombinedDeliveryLog combinedDeliveryLog = clientRequestIdToCombinedDeliveryLog.get(clientRequestId);
            if (debugIds.matches(combinedDeliveryLog)) {
                LOGGER.info("CombineDeliveryLog onTimer collect ts={}\nwatermark={}\ncombinedDeliveryLog={}",
                        ctx.timestamp(), ctx.timerService().currentWatermark(), combinedDeliveryLog);
            }
            out.collect(combinedDeliveryLog);
            clientRequestIdToCombinedDeliveryLog.remove(clientRequestId);
        }
        timerToClientRequestId.remove(timestamp);
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        clientRequestIdToCombinedDeliveryLog = getRuntimeContext().getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>(
                        "client-request-id-to-combined-delivery-log",
                        Types.STRING,
                        TypeInformation.of(CombinedDeliveryLog.class)));
        timerToClientRequestId = getRuntimeContext().getMapState(
                // TODO(PRO-1683) - add caches back in.
                new MapStateDescriptor<>(
                        "timer-to-client-request-id",
                        Types.LONG,
                        Types.LIST(Types.STRING)));
    }

    static CombinedDeliveryLog.Builder mergeDeliveryLog(CombinedDeliveryLog.Builder builder, DeliveryLog deliveryLog) {
        switch (deliveryLog.getExecution().getExecutionServer()) {
            case SDK:
                if (builder.hasSdk() && !hasSameRequestId(builder.getSdk(), deliveryLog)) {
                    LOGGER.warn("Encountered multiple SDK DeliveryLogs with the same clientRequestId; first={}, second={}",
                            builder.getSdk(), deliveryLog);
                }
                // This code is in here more as future proofing.  E.g. if we accidentally send sdk traffic again through production.
                if (!builder.hasSdk() || getTrafficPriority(deliveryLog) >= getTrafficPriority(builder.getSdk())) {
                    builder.setSdk(deliveryLog);
                }
                break;
            case API:
            case SIMPLE_API:
                // TODO - after enough time has passed, we can change UNKNOWN_EXECUTION_SERVER to behave differently.
            case UNKNOWN_EXECUTION_SERVER:
                // TODO - factor in shadow traffic.

                if (builder.hasApi() && !hasSameRequestId(builder.getApi(), deliveryLog)) {
                    LOGGER.warn("Encountered multiple API DeliveryLogs with the same clientRequestId; first={}, second={}",
                            builder.getApi(), deliveryLog);
                }
                if (!builder.hasApi() || getTrafficPriority(deliveryLog) >= getTrafficPriority(builder.getApi())) {
                    builder.setApi(deliveryLog);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported ExecutionServer=" + deliveryLog.getExecution().getExecutionServer());
        }
        builder = fixSdkResponse(builder);
        return builder;
    }

    private static boolean hasSameRequestId(DeliveryLog left, DeliveryLog right) {
        return left.getRequest().getRequestId().equals(right.getRequest().getRequestId());
    }

    /**
     * Fixes SDK Response to use the API Response's pagingId.
     *
     * SDKs do not generating pagingIds for SDK DeliveryLogs.  Our duplicate content ID checks would benefit from
     * having paging IDs on joined events.  Short-term, we can either fix it here or in merge details stage.
     *
     * This is a hack.  It's better than nothing.  The longer-term solution depends on how much we want to support in
     * the SDK directly.  E.g. do we actually implement a full paging id solution in each SDK?  Dan would prefer that
     * we do not do this.  We want to keep the SDK layer small.
     */
    private static CombinedDeliveryLog.Builder fixSdkResponse(CombinedDeliveryLog.Builder builder) {
        if (builder.hasApi()
                && builder.hasSdk()
                && builder.getSdk().getResponse().getPagingInfo().getPagingId().isEmpty()
                && !builder.getApi().getResponse().getPagingInfo().getPagingId().isEmpty()) {
            builder.getSdkBuilder().getResponseBuilder().getPagingInfoBuilder().setPagingId(
                    builder.getApi().getResponse().getPagingInfo().getPagingId());
        }
        return builder;
    }
}