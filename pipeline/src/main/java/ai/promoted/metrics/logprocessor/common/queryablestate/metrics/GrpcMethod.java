package ai.promoted.metrics.logprocessor.common.queryablestate.metrics;

import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import java.util.Objects;

/** Knows how to extract information about a single grpc method. */
class GrpcMethod {
  private final String serviceName;
  private final String methodName;
  private final MethodType type;

  static GrpcMethod of(MethodDescriptor<?, ?> method) {
    String serviceName = MethodDescriptor.extractFullServiceName(method.getFullMethodName());

    // Full method names are of the form: "full.serviceName/MethodName". We extract the last part.
    String methodName = method.getFullMethodName().substring(serviceName.length() + 1);
    return new GrpcMethod(serviceName, methodName, method.getType());
  }

  private GrpcMethod(String serviceName, String methodName, MethodType type) {
    this.serviceName = serviceName;
    this.methodName = methodName;
    this.type = type;
  }

  public String serviceName() {
    return serviceName;
  }

  public String methodName() {
    return methodName;
  }

  public String type() {
    return type.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GrpcMethod that = (GrpcMethod) o;
    return Objects.equals(serviceName, that.serviceName)
        && Objects.equals(methodName, that.methodName)
        && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName, methodName, type);
  }
}
