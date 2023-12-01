package ai.promoted.metrics.logprocessor.job.sqlrunner;

import org.apache.flink.table.factories.ModuleFactory;
import org.apache.flink.table.module.Module;

public class PromotedModuleFactory implements ModuleFactory {
  @Override
  public Module createModule(Context context) {
    return new PromotedModule();
  }

  @Override
  public String factoryIdentifier() {
    return "promoted";
  }
}
