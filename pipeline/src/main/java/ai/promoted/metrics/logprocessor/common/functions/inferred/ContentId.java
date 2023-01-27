package ai.promoted.metrics.logprocessor.common.functions.inferred;

import static ai.promoted.metrics.logprocessor.common.util.StringUtil.isBlank;
import static ai.promoted.metrics.logprocessor.common.util.StringUtil.isNonEmptyAndEqual;

import ai.promoted.metrics.error.MismatchError;
import ai.promoted.metrics.logprocessor.common.functions.SerializableFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializablePredicate;
import ai.promoted.metrics.logprocessor.common.functions.SerializableToLongFunction;
import ai.promoted.metrics.logprocessor.common.functions.SerializableTriFunction;
import ai.promoted.proto.event.TinyEvent;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO - rename to ContentIdProcessFunction.
/** This abstract base class handles content id based inferred joins. */
public abstract class ContentId<RHS extends GeneratedMessageV3> extends BaseInferred<RHS> {
    private static final Logger LOGGER = LogManager.getLogger(ContentId.class);
    private SerializableFunction<TinyEvent, ? extends Collection<String>> leftContentsGetter;
    private SerializableFunction<RHS, ? extends Collection<String>> rightContentsGetter;

    /**
     * @param leftContentsGetter Function to extract any content ids from the LHS.
     * @param rightContentsGetter Function to extract any content ids from the RHS.
     * {@inheritDoc}
     */
    protected ContentId(
            String name,
            SerializablePredicate<TinyEvent> hasLeft,
            SerializableFunction<TinyEvent, ? extends Collection<String>> leftContentsGetter,
            SerializableFunction<RHS, String> rightPKeyGetter,
            SerializableFunction<RHS, String> rightJoinIdGetter,
            SerializableFunction<RHS, ? extends Collection<String>> rightContentsGetter,
            SerializableToLongFunction<RHS> rightLogTimeGetter,
            SerializableTriFunction<TinyEvent.Builder, RHS, BiConsumer<OutputTag<MismatchError>, MismatchError>, TinyEvent.Builder> join,
            boolean includeKeyAsScope,
            Options options) {
        super(name, hasLeft, rightPKeyGetter, rightJoinIdGetter, join, rightLogTimeGetter, includeKeyAsScope, options);
        this.leftContentsGetter = leftContentsGetter;
        this.rightContentsGetter = rightContentsGetter;
    }

    @Override
    protected List<TinyEvent> inferJoin(String scopeKey, RHS rhs, Context ctx) throws Exception {
        boolean debugLogging = debugIdsMatch(rhs);
        List<TinyEvent> scope = getTinyEventsForScope(scopeKey);
        if (scope == null || scope.isEmpty()) {
            if (debugLogging) {
                LOGGER.info("{} ContentId.inferJoin no scopes: scopeKey={}\nrhs= {}", name, scopeKey, rhs);
            }
            return ImmutableList.of();
        }

        // The following filter is just an optimization.  If a contentId is specified on the action, narrow down all
        // scopes so at least one ID matches.
        Collection<String> rhsContentIds = rightContentsGetter.apply(rhs);
        // If no contentIds, skip.
        if (!rhsContentIds.isEmpty()) {
            // Maybe this is code that handles the out of order contentId.
            scope = scope.stream()
                .filter(hasAnyMatchingContentId(rhsContentIds))
                .collect(Collectors.toList());
        }
        if (debugLogging) {
            LOGGER.info("{} ContentId.inferJoin scopes: scopeKey={}\n scope={}\nrhs= {}", name, scopeKey, scope, rhs);
        }
        return super.inferJoin(scope, rhs, ctx);
    }

    private Predicate<TinyEvent> hasAnyMatchingContentId(Collection<String> rhsContentIds) {
        return (flat) -> {
            Collection<String> lhsContentIds = leftContentsGetter.apply(flat);
            return rhsContentIds.stream().anyMatch(rhsContentId -> lhsContentIds.stream().anyMatch(lhsContentId ->
                    !isBlank(lhsContentId) && !isBlank(rhsContentId) && Objects.equal(lhsContentId, rhsContentId)));
        };
    }
}
