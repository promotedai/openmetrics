package ai.promoted.metrics.logprocessor.common.job;

import ai.promoted.metrics.logprocessor.common.constant.Constants;
import ai.promoted.metrics.logprocessor.common.util.DebugIds;
import ai.promoted.metrics.logprocessor.common.util.StringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;

/** ABC for a Flink job. */
public abstract class BaseFlinkJob implements CompositeFlinkSegment, Callable<Integer> {
  private static final Logger LOGGER = LogManager.getLogger(BaseFlinkJob.class);

  @CommandLine.Option(
      names = {"--jobName"},
      defaultValue = "",
      description =
          "The name of the job (excluding the label prefix).  This can be used to run multiple jobs with the same label and different outputs.  Defaults to empty string which uses the default for the job.")
  public String jobName = "";

  @Option(
      names = {"--jobLabel"},
      defaultValue = Constants.LIVE_LABEL,
      description = "Label for Flink job.  Defaults to 'live'")
  public String jobLabel = Constants.LIVE_LABEL;

  @Option(
      names = {"--topicInputLabel"},
      description =
          "Highest priority override for the input job label."
              + "(then --inputLabel and then --jobLabel).  Can be used to support running a job on a specific label "
              + "but have a subset of inputs from a different label.  Empty string means no override."
              + "Defaults to empty string (no override)")
  Map<String, String> topicInputLabel = ImmutableMap.of();

  @Option(
      names = {"--inputLabel"},
      defaultValue = "",
      description =
          "Overrides the Kafka input label "
              + "(defaults to --jobLabel).  Can be used for cases like --jobLabel='blue.canary' and "
              + "--inputLabel='blue'.  Empty string means no override.  Cannot be used to override to empty "
              + "string (not useful now).  --topicInputLabel takes priority over this flag.  Defaults to empty string (no override)")
  public String inputLabel = "";

  @Option(
      names = {"--maxParallelism"},
      required = true,
      description =
          "The maxParallelism value for this job.  See https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/execution/parallel/ for more details.  Required since this value is important and should not change between savepoints.")
  public int maxParallelism;

  @Option(
      names = {"--disableAutoGeneratedUIDs"},
      negatable = true,
      description = "Whether to disableAutoGeneratedUIDs.")
  public boolean disableAutoGeneratedUIDs = false;

  // TODO - optimize the checkpointing interval.
  @Option(
      names = {"--checkpointInterval"},
      defaultValue = "PT5M",
      description = "Checkpoint interval duration.  Default=PT5M.")
  public Duration checkpointInterval = Duration.ofMinutes(5);

  // TODO - optimize the checkpointing timeout.
  @Option(
      names = {"--checkpointTimeoutDuration"},
      defaultValue = "PT1H",
      description = "Checkpoint timeout in milliseconds.  Default=3600000")
  public Duration checkpointTimeout = Duration.ofHours(1);

  // Build a list of sink transformations for our tests.
  // All sinks should be in sinkTransformations.
  public ArrayList<Transformation<?>> sinkTransformations = new ArrayList<>();

  // This is just for raw log jobs.  We don't expect to set this above 1.
  // TODO(PRO-1674): remove the job specific arg names
  @Option(
      names = {"--parallelism", "--rawJobParallelism", "--joinJobParallelism"},
      defaultValue = "1",
      description = "The parallelism value for this job.  The lowest priority setting.  Default=1")
  protected int parallelism = 1;

  @Option(
      names = {"--defaultSourceParallelism"},
      defaultValue = "0",
      description =
          "The default parallelism to use for sinks.  --operatorParallelismMultiplier is higher priority.  --parallelism is lower priority.  Default=0 (use default parallelism)")
  protected Integer defaultSourceParallelism = 0;

  @Option(
      names = {"--defaultSinkParallelism"},
      defaultValue = "0",
      description =
          "The default parallelism to use for sinks.  --operatorParallelismMultiplier is higher priority.  --parallelism is lower priority.  Default=0 (use default parallelism)")
  protected Integer defaultSinkParallelism = 0;

  //////// CHECKPOINT OPTIONS
  @Option(
      names = {"--operatorParallelism"},
      description =
          "Map of operator name to operator parallelism. Since the name is not unique, all operators with the given name will be set.  Takes the highest priority over other related flags.  Default=Empty (which means fall back to the other parallelism fields)")
  @VisibleForTesting
  public Map<String, Integer> operatorParallelism = ImmutableMap.of();

  @Option(
      names = {"--operatorParallelismMultiplier"},
      description =
          "Map of operator name to operator parallelism multipliers. Since the name is not unique, all operators with the given name will be set.  Higher priority than most flags (not --operatorParallelism).  Default=Empty (which means fall back to the other parallelism fields)")
  Map<String, Float> operatorParallelismMultiplier = ImmutableMap.of();

  @Option(
      names = {"--no-enableObjectReuse"},
      negatable = true,
      description = "Whether to enableObjectReuse.")
  boolean enableObjectReuse = true;

  @Option(
      names = {"--minPauseBetweenCheckpoints"},
      defaultValue = "5000",
      description = "minPauseBetweenCheckpoints in milliseconds.")
  int minPauseBetweenCheckpoints;

  @Option(
      names = {"--tolerableCheckpointFailureNumber"},
      defaultValue = "3",
      description =
          "Tolerable checkpoint failures.  Default=3.  Arbitrary value.  We want to handle two random failures in a row before killing the job.")
  int tolerableCheckpointFailureNumber;

  @Option(
      names = {"--checkpointingMode"},
      defaultValue = "EXACTLY_ONCE",
      description = "Checkpointing mode.")
  CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;

  //////// DEBUG OPTIONS
  @Option(
      names = {"--debugUserIds"},
      description =
          "Set of userIds substrings to log more join debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.")
  public Set<String> debugUserIds = ImmutableSet.of();

  @Option(
      names = {"--debugAnonUserIds"},
      description =
          "Set of anonUserIds substrings to log more join debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.")
  public Set<String> debugAnonUserIds = ImmutableSet.of();

  @Option(
      names = {"--debugSessionIds"},
      description =
          "Set of sessionIds substrings to log more join debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.")
  public Set<String> debugSessionIds = ImmutableSet.of();

  @Option(
      names = {"--debugViewIds"},
      description =
          "Set of viewIds substrings to log more join debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.")
  public Set<String> debugViewIds = ImmutableSet.of();

  @Option(
      names = {"--debugRequestIds"},
      description =
          "Set of requestIds substrings to log more join debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.")
  public Set<String> debugRequestIds = ImmutableSet.of();

  @Option(
      names = {"--debugInsertionIds"},
      description =
          "Set of insertionIds substrings to log more join debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.")
  public Set<String> debugInsertionIds = ImmutableSet.of();

  @Option(
      names = {"--debugImpressionIds"},
      description =
          "Set of impressionIds substrings to log more join debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.")
  public Set<String> debugImpressionIds = ImmutableSet.of();

  @Option(
      names = {"--debugActionIds"},
      description =
          "Set of actionIds substrings to log more join debug logs. Acts as an OR with other debug fields. Used to track down missing flattened events.")
  public Set<String> debugActionIds = ImmutableSet.of();

  // where it is used (i.e. DebugIds debugIds = getDebugIds()).
  private transient DebugIds cachedDebugIds;

  /** Main for running the Flink job. Will throw a RTE until overridden. */
  public static void main(String[] args) {
    /* Usually, this is implemented as: executeMain(new MyJob(), args); */
    throw new UnsupportedOperationException("Statically define main in your job class.");
  }

  public static void executeMain(BaseFlinkJob job, String[] args) {
    CommandLine commandLine = new CommandLine(job).setTrimQuotes(true);
    // Hacky way to change the behaviour of picocli because
    // we want the method to fail fast with exceptions rather than returning an exit code.
    commandLine.setErr(
        // a dummy PrintWriter that throws an exception when print(String) is invoked.
        new PrintWriter(System.out) {
          @Override
          public void print(String s) {
            throw new ExecutionException(commandLine, s);
          }
        });
    commandLine.setExecutionExceptionHandler(
        (e, commandLine1, parseResult) -> {
          throw e;
        });
    commandLine.setParameterExceptionHandler(
        (e, strings) -> {
          throw e;
        });
    int exitCode = commandLine.execute(args);
    if (exitCode != 0) {
      throw new RuntimeException("Picocli failed to throw an exception on failure");
    }
  }

  protected abstract void startJob() throws Exception;

  @Override
  public Integer call() throws Exception {
    validateArgs();
    startJob();
    return 0;
  }

  @Override
  public void validateArgs() {
    CompositeFlinkSegment.super.validateArgs();
    Preconditions.checkArgument(maxParallelism > 0, "--maxParalllelism must be set");
  }

  /** Returns if this job is the live/prod version of a job. */
  public boolean isLiveLabel() {
    return "".equals(jobLabel) || Constants.LIVE_LABEL.equals(jobLabel);
  }

  protected String getJobName() {
    return prefixJobLabel(jobName.isBlank() ? getDefaultBaseJobName() : jobName);
  }

  /** Returns the default, base name of the Flink job. */
  protected abstract String getDefaultBaseJobName();

  /** Returns the job label for this job. If live/prod, it will return an empty string. */
  public String getJobLabel() {
    return isLiveLabel() ? "" : jobLabel;
  }

  /**
   * Returns the job label to use for inputs into the job. This allows us to create an upstream job
   * using a different label.
   */
  public String getInputLabel(String id) {
    String topicOverride = topicInputLabel.get(id);
    if (!Strings.isNullOrEmpty(topicOverride)) {
      return topicOverride;
    }
    return StringUtil.firstNotEmpty(inputLabel, getJobLabel());
  }

  private String prefixJobLabel(String input) {
    return (isLiveLabel() ? "" : jobLabel + ".") + input;
  }

  /** Returns the Kafka consumer group id for this job. */
  // This is located here due to the scope of the jobLabel argument.
  public String toKafkaConsumerGroupId(String baseKafkaGroupId) {
    return prefixJobLabel(baseKafkaGroupId);
  }

  public <T> SingleOutputStreamOperator<T> add(SingleOutputStreamOperator<T> operator, String uid) {
    operator = operator.uid(uid).name(uid);
    Optional<Integer> operatorParallelism = getOperatorParallelism(uid);
    if (operatorParallelism.isPresent()) {
      LOGGER.info("operator {} parallelism={}", uid, operatorParallelism.get());
      operator = operator.setParallelism(operatorParallelism.get());
    }
    return operator;
  }

  public <T> SingleOutputStreamOperator<T> add(DataStreamSource<T> source, String uid) {
    SingleOutputStreamOperator<T> out = source.uid(uid).name(uid);
    int operatorParallelism = getSourceParallelism(uid);
    LOGGER.info("source {} parallelism={}", uid, operatorParallelism);
    return out.setParallelism(operatorParallelism);
  }

  public <T> DataStreamSink<T> add(DataStreamSink<T> sink, String uid) {
    sink = sink.uid(uid).name(uid);
    int sinkParallelism = getSinkParallelism(uid);
    if (sinkParallelism == 0) {
      throw new IllegalArgumentException("sinkParallelism should be >0; uid=" + uid);
    }
    LOGGER.info("sink {} parallelism={}", uid, sinkParallelism);
    sink = sink.setParallelism(sinkParallelism);
    addSinkTransformation(sink.getTransformation());
    return sink;
  }

  // TODO Use name instead of uid since not all operators have uid.
  @VisibleForTesting
  protected Optional<Integer> getOperatorParallelism(String operatorName) {
    Integer p = operatorParallelism.get(operatorName);
    if (p != null) {
      return Optional.of(rangeParallelism(p));
    }
    Float multiplier = operatorParallelismMultiplier.get(operatorName);
    if (multiplier != null) {
      return Optional.of(rangeParallelism(Math.round(multiplier * parallelism)));
    }
    return Optional.empty();
  }

  @VisibleForTesting
  int getSourceParallelism(String uid) {
    Optional<Integer> p = getOperatorParallelism(uid);
    if (p.isPresent()) {
      return p.get();
    } else if (defaultSourceParallelism > 0) {
      return rangeParallelism(defaultSourceParallelism);
    } else {
      return rangeParallelism(parallelism);
    }
  }

  @VisibleForTesting
  public int getSinkParallelism(String uid) {
    Optional<Integer> p = getOperatorParallelism(uid);
    if (p.isPresent()) {
      return p.get();
    } else if (defaultSinkParallelism > 0) {
      return rangeParallelism(defaultSinkParallelism);
    } else {
      return rangeParallelism(parallelism);
    }
  }

  private int rangeParallelism(int p) {
    return Math.max(1, Math.min(maxParallelism, p));
  }

  protected void addSinkTransformation(@Nullable Transformation transformation) {
    addSinkTransformation(Optional.ofNullable(transformation));
  }

  protected void addSinkTransformation(Optional<Transformation> transformation) {
    transformation.ifPresent(sinkTransformations::add);
  }

  protected void addSinkTransformations(Iterable<Transformation> transformations) {
    transformations.forEach(sinkTransformations::add);
  }

  /**
   * Returns DebugIds from command line arguments. Due to serialization with flink, it's best to
   * instantiate a reference local to the function where it's used.
   */
  protected DebugIds getDebugIds() {
    if (cachedDebugIds == null) {
      cachedDebugIds =
          DebugIds.builder()
              .setUserIds(debugUserIds)
              .setAnonUserIds(debugAnonUserIds)
              .setSessionIds(debugSessionIds)
              .setViewIds(debugViewIds)
              .setRequestIds(debugRequestIds)
              .setInsertionIds(debugInsertionIds)
              .setImpressionIds(debugImpressionIds)
              .setActionIds(debugActionIds)
              .build();
    }
    return cachedDebugIds;
  }

  public <T> SingleOutputStreamOperator<T> addTextLogDebugIdsOperator(
      SingleOutputStreamOperator<T> input, Class<T> clazz, String label) {
    return (SingleOutputStreamOperator<T>)
        addTextLogDebugIdsOperatorAsDataStream(input, clazz, label);
  }

  /** If any debugIds are enabled, adds an operator around input that text logs the records. */
  public <T> DataStream<T> addTextLogDebugIdsOperatorAsDataStream(
      DataStream<T> input, Class<T> clazz, String label) {
    final DebugIds debugIds = getDebugIds();
    // Call this here to catch issues earlier in automated tests.
    getDebugIdsMatchesMethodFor(clazz);
    if (debugIds.hasAnyIds()) {
      // Uses reflection to reduce number of arguments that are needed.
      // This is fine since debugIds should only be used when debugging.
      return add(
          input.process(
              new ProcessFunction<T, T>() {
                transient Method matchesDebugIdsMethod;

                public void open(Configuration parameters) throws Exception {
                  matchesDebugIdsMethod = getDebugIdsMatchesMethodFor(clazz);
                }

                @Override
                public void processElement(
                    T record, ProcessFunction<T, T>.Context context, Collector<T> collector)
                    throws Exception {
                  Object matchesDebugId = matchesDebugIdsMethod.invoke(debugIds, record);
                  Preconditions.checkArgument(
                      matchesDebugId instanceof Boolean,
                      "matchesDebugId should be a Boolean, actual=%s",
                      matchesDebugId.getClass());
                  if ((Boolean) matchesDebugId) {
                    LOGGER.info(
                        "{} debug ids; record={}, time={}, watermark={}",
                        label,
                        record,
                        context.timestamp(),
                        context.timerService().currentWatermark());
                  }
                  collector.collect(record);
                }
              }),
          "with-debugids-textlog-" + label);
    } else {
      return input;
    }
  }

  private static Method getDebugIdsMatchesMethodFor(Class argClass) {
    for (Method method : DebugIds.class.getDeclaredMethods()) {
      if (method.getName().equals("matches")) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 1 && parameterTypes[0].equals(argClass)) {
          return method;
        }
      }
    }
    throw new UnsupportedOperationException(
        "Unable to find `DebugIds.matches` method with arg " + argClass);
  }

  /**
   * Configures a StreamExecutionEnvironment.
   *
   * @param env the environment to configure
   * @param parallelism the parallelism for the job
   * @param maxParallelism the maxParallelism for the job
   */
  public void configureExecutionEnvironment(
      StreamExecutionEnvironment env, int parallelism, int maxParallelism) {
    getProtoClasses()
        .forEach(c -> FlinkSegment.optionalRegisterProtobufSerializer(env.getConfig(), c));

    if (disableAutoGeneratedUIDs) {
      env.getConfig().disableAutoGeneratedUIDs();
    }

    env.setParallelism(parallelism);
    if (maxParallelism > 0) {
      env.setMaxParallelism(maxParallelism);
    }

    if (enableObjectReuse) {
      env.getConfig().enableObjectReuse();
    }

    if (checkpointInterval.toMillis() > 0) {
      env.enableCheckpointing(checkpointInterval.toMillis());
    }
    env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
    // TODO - evaluate if we want setMinPauseBetweenCheckpoints.
    if (minPauseBetweenCheckpoints > 0) {
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
    }
    if (checkpointTimeout.toMillis() > 0) {
      env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout.toMillis());
    }
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(tolerableCheckpointFailureNumber);

    // add job specific directory for checkpoint path when using FileSystemStorage
    CheckpointStorage checkpointStorage = env.getCheckpointConfig().getCheckpointStorage();
    if (checkpointStorage instanceof FileSystemCheckpointStorage) {
      String jobSpecificPath =
          ((FileSystemCheckpointStorage) env.getCheckpointConfig().getCheckpointStorage())
                  .getCheckpointPath()
                  .toUri()
              + "/"
              + getJobName();
      env.getCheckpointConfig().setCheckpointStorage(jobSpecificPath);
      LOGGER.info("Set checkpoint path to " + jobSpecificPath);
    }

    // add job specific directory for default savepoint path if it's set
    String defaultSavepointPath =
        StringUtil.firstNotNull(
            null == env.getDefaultSavepointDirectory()
                ? null
                : env.getDefaultSavepointDirectory().toUri().toString(),
            env.getConfiguration().get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
    if (null != defaultSavepointPath) {
      String jobSpecificPath = defaultSavepointPath + "/" + getJobName();
      // This caused a bug in https://issues.apache.org/jira/browse/FLINK-23515
      // env.setDefaultSavepointDirectory(jobSpecificPath);
      // LOGGER.info("Set default savepoint path to " + jobSpecificPath);
    }
  }
}
