package ai.promoted.metrics.logprocessor.job.join.common;

import com.google.common.base.Joiner;
import java.util.stream.Collectors;
import org.apache.flink.table.types.DataType;

public class JoinUtil {
  /** Returns sql select statement like "{prefix}.{field1}, {prefix}.{field2}, ...". */
  public static String toSelectFieldSql(DataType rowType, String prefix) {
    return Joiner.on(", ")
        .join(
            DataType.getFieldNames(rowType).stream()
                .map(name -> prefix + name)
                .collect(Collectors.toList()));
  }

  public static String toCastNullSql(DataType rowType) {
    return Joiner.on(", ")
        .join(
            DataType.getFieldDataTypes(rowType).stream()
                .map(type -> "CAST(NULL AS " + type.getLogicalType().asSummaryString() + ")")
                .collect(Collectors.toList()));
  }

  /** Returns type SQL that is used in a CAST. */
  public static String getSchemaSql(DataType rowType) {
    String schema = rowType.toString();
    if (schema.endsWith(" NOT NULL")) {
      schema = schema.substring(0, schema.lastIndexOf(" NOT NULL"));
    }
    return schema;
  }
}
