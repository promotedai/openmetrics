package ai.promoted.metrics.logprocessor.common.error;

import ai.promoted.metrics.error.MismatchError;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

// TODO - add the log file name to the parquet file.

/** A class containing an OutputTag. */
public final class MismatchErrorTag {
  public static final OutputTag<MismatchError> TAG =
      new OutputTag<MismatchError>("mismatch-error", TypeInformation.of(MismatchError.class));
}
