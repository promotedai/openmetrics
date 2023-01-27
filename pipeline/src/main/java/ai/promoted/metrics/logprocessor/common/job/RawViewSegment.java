package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.functions.KeepFirstRow;
import ai.promoted.metrics.logprocessor.common.functions.KeyUtil;
import ai.promoted.proto.event.AutoView;
import ai.promoted.proto.event.View;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import picocli.CommandLine;

import java.time.Duration;
import java.util.List;

/**
 * A FlinkSegment that provides views as a source.  It's a separate FlinkSegment
 * to isolate the flags to just jobs that use the deduplication source.
 */
public class RawViewSegment implements FlinkSegment {

    @CommandLine.Option(names = {"--keepFirstViewDuration"}, defaultValue = "PT6H", description = "The duration to keep track of recent Views and AutoViews.  This is used to de-duplicate raw inputs.  Default=PT6H.  Java8 Duration parse format.")
    public Duration keepFirstViewDuration = Duration.parse("PT6H");

    private final BaseFlinkJob baseFlinkJob;

    public RawViewSegment(BaseFlinkJob baseFlinkJob) {
        this.baseFlinkJob = baseFlinkJob;
    }

    @Override
    public void validateArgs() {
        // Do nothing.
    }

    public DataStream<View> getDeduplicatedView(DataStream<View> view) {
        return baseFlinkJob.add(
                view.keyBy(KeyUtil.viewKeySelector)
                        .process(new KeepFirstRow<>("view", keepFirstViewDuration),
                                TypeInformation.of(View.class)),
                "keep-first-view");
    }

    public DataStream<AutoView> getDeduplicatedAutoView(DataStream<AutoView> autoView) {
        return baseFlinkJob.add(
                autoView.keyBy(KeyUtil.autoViewKeySelector)
                        .process(new KeepFirstRow<>("autoview", keepFirstViewDuration),
                                TypeInformation.of(AutoView.class)),
                "keep-first-auto-view");
    }

    @Override
    public List<Class<? extends GeneratedMessageV3>> getProtoClasses() {
        return ImmutableList.of(View.class);
    }
}
