package ai.promoted.metrics.logprocessor.job.sqlrunner;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

public class PromotedUdfs implements Serializable {

  /** Returns the array if it's not null. Otherwise, returns an empty array of the same type. */
  public static class OrEmptyArray extends ScalarFunction {
    private DataType type;

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .outputTypeStrategy(
              callContext -> {
                type = callContext.getArgumentDataTypes().get(0);
                return Optional.of(callContext.getArgumentDataTypes().get(0));
              })
          .build();
    }

    public final Object[] eval(Object[] array) {
      if (null == array) {
        return (Object[])
            Array.newInstance(type.getLogicalType().getChildren().get(0).getDefaultConversion(), 0);
      }
      return array;
    }
  }

  /**
   * Iterate the input row array and keeps the items with the field value exists (or not exists) in
   * the given target array.
   */
  public static class FilterRowArray extends IdenticalTypeScalarFunction {
    public final Row[] eval(Row[] array, String field, boolean flag, Object... targets) {
      Set<Object> set = new HashSet<>(Arrays.asList(targets));
      return Arrays.stream(array)
          .filter(row -> set.contains(row.getField(field)) == flag)
          .toArray(Row[]::new);
    }
  }

  /**
   * Concatenate arrays of the same type.
   *
   * <p>TODO replace this with built-in ARRAY_CONCAT once switch to Flink 1.18
   */
  public static class ArrayConcat extends IdenticalTypeScalarFunction {
    @SafeVarargs
    public final <T> T[] eval(T[] first, T[]... rest) {
      int totalLength = first.length;
      for (T[] array : rest) {
        totalLength += array.length;
      }
      T[] result = Arrays.copyOf(first, totalLength);
      int offset = first.length;
      for (T[] array : rest) {
        System.arraycopy(array, 0, result, offset, array.length);
        offset += array.length;
      }
      return result;
    }
  }

  /**
   * Checks the model id and action type and returns the first existing one and its credit value.
   * E.g., FIRST_MODEL_AND_CREDIT(attributed_action, CAST(2 AS BIGINT), 'NAVIGATE','CUSTOM') is
   * equivalent to the combination of
   *
   * <p>reduce(attributed_action, false, (s, e) -> s OR (e.attribution.model_id = 2 AND
   * e.action.action_type IN ('NAVIGATE', 'CUSTOM')), s -> s) AS exist
   *
   * <p>and
   *
   * <p>reduce(attributed_action, 0, (s, e) -> IF(s > 0, s, IF(e.attribution.model_id = 2 AND
   * e.action.action_type IN ('NAVIGATE', 'CUSTOM'), e.attribution.credit_millis, 0)), s -> s) AS
   * credit_millis
   */
  public static class FirstModelAndCredit extends ScalarFunction {
    public final Row eval(Row[] attributedActionArray, Long modelId, Object... actionTypes) {
      if (null == attributedActionArray) {
        return Row.of(false, -1);
      }
      List<Object> actionTypeList = Arrays.asList(actionTypes);
      boolean exist = false;
      int creditMillis = 0;
      for (Row attributedAction : attributedActionArray) {
        if (Objects.equals(modelId, getNestedValue(attributedAction, "attribution", "model_id"))
            && actionTypeList.contains(getNestedValue(attributedAction, "action", "action_type"))) {
          exist = true;
          Integer value = getNestedValue(attributedAction, "attribution", "credit_millis");
          if (null != value) {
            creditMillis = value;
          }
        }
        if (exist && creditMillis > 0) { // short circuit
          return Row.of(true, creditMillis);
        }
      }
      return Row.of(exist, creditMillis);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .outputTypeStrategy(
              callContext ->
                  Optional.of(
                      DataTypes.ROW(
                          DataTypes.FIELD("exist", DataTypes.BOOLEAN()),
                          DataTypes.FIELD("credit_millis", DataTypes.INT()))))
          .build();
    }
  }

  /**
   * Filters rows in an array based on two row fields.
   *
   * <p>E.g., FILTER_ROW_ARRAY_BY_TWO_FIELDS(features, TRUE, 'value', FALSE, ARRAY [CAST(0.0 AS
   * FLOAT)], 'key', TRUE, ARRAY [1, 2]) is equivalent to applying the filter condition (row ->
   * row.value NOT IN ARRAY[0.0] AND row.key in ARRAY[1, 2]).
   */
  public static class FilterRowArrayByTwoFields extends IdenticalTypeScalarFunction {
    public final Row[] eval(
        Row[] array,
        boolean isAnd,
        String field1,
        boolean flag1,
        Object[] targets1,
        String field2,
        boolean flag2,
        Object[] targets2) {
      Set<Object> targets1Set = new HashSet<>(Arrays.asList(targets1));
      Set<Object> targets2Set = new HashSet<>(Arrays.asList(targets2));
      return Arrays.stream(array)
          .filter(
              row -> {
                boolean b1 = (flag1 == targets1Set.contains(getNestedValue(row, field1)));
                boolean b2 = (flag2 == targets2Set.contains(getNestedValue(row, field2)));
                return isAnd ? b1 && b2 : b1 || b2;
              })
          .toArray(Row[]::new);
    }
  }

  /**
   * Converts a feature array of rows to a feature map.
   *
   * <p>E.g., TO_FEATURE_MAP(features, 'key', 'value') will extract the 'key' and 'value' fields
   * from the features row array and construct a map structure.
   */
  public static class ToFeatureMap extends ScalarFunction {
    public final Map<Long, Float> eval(Row[] featureArray, String keyField, String valueField) {
      Map<Long, Float> featureMap = new HashMap<>();
      for (Row row : featureArray) {
        featureMap.put(getNestedValue(row, keyField), getNestedValue(row, valueField));
      }
      return featureMap;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .outputTypeStrategy(
              callContext -> Optional.of(DataTypes.MAP(DataTypes.BIGINT(), DataTypes.FLOAT())))
          .build();
    }
  }

  /** Scalar function that returns the same type as the first argument. */
  public abstract static class IdenticalTypeScalarFunction extends ScalarFunction {
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .outputTypeStrategy(callContext -> Optional.of(callContext.getArgumentDataTypes().get(0)))
          .build();
    }
  }

  public static <T> T getNestedValue(Row row, String... fields) {
    Row nestedRow = row;
    for (int i = 0; i < fields.length - 1; ++i) {
      if (null == nestedRow) {
        return null;
      }
      nestedRow = nestedRow.getFieldAs(fields[i]);
    }
    return null == nestedRow ? null : nestedRow.getFieldAs(fields[fields.length - 1]);
  }
}
