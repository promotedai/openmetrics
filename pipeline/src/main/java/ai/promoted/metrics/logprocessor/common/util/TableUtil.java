package ai.promoted.metrics.logprocessor.common.util;

import java.util.Map;
import java.util.stream.Collectors;

public class TableUtil {

  public static String databaseAndTableName(String database, String table) {
    return database + "." + table;
  }

  public static String fullTableName(String catalog, String database, String table) {
    return String.format("`%s`.`%s`.`%s`", catalog, database, table);
  }

  public static String getLabeledDatabaseName(String databaseName, String label) {
    return databaseName + "_" + label;
  }

  public static String genTableOptionsStr(Map<String, String> otherOptions) {
    return otherOptions.entrySet().stream()
        .map(entry -> String.format("'%s'='%s'", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(","));
  }
}
