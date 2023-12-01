package ai.promoted.metrics.logprocessor.job.sqlrunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

public class PromotedModule implements Module {

  private static final Map<String, FunctionDefinition> functionDefinitionMap = new HashMap<>();

  static {
    functionDefinitionMap.put("OR_EMPTY_ARRAY", new PromotedUdfs.OrEmptyArray());
    functionDefinitionMap.put("FILTER_ROW_ARRAY", new PromotedUdfs.FilterRowArray());
    functionDefinitionMap.put("ARRAY_CONCAT", new PromotedUdfs.ArrayConcat());
    functionDefinitionMap.put("FIRST_MODEL_AND_CREDIT", new PromotedUdfs.FirstModelAndCredit());
    functionDefinitionMap.put(
        "FILTER_ROW_ARRAY_BY_TWO_FIELDS", new PromotedUdfs.FilterRowArrayByTwoFields());
    functionDefinitionMap.put("TO_FEATURE_MAP", new PromotedUdfs.ToFeatureMap());
  }

  @Override
  public Set<String> listFunctions() {
    return functionDefinitionMap.keySet();
  }

  @Override
  public Set<String> listFunctions(boolean includeHiddenFunctions) {
    return functionDefinitionMap.keySet();
  }

  @Override
  public Optional<FunctionDefinition> getFunctionDefinition(String name) {
    return Optional.of(functionDefinitionMap.get(name.toUpperCase()));
  }
}
