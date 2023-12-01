package ai.promoted.metrics.logprocessor.common.queryablestate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Reflector {

  private final ConcurrentHashMap<String, Field> fieldCache = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, Method> methodCache = new ConcurrentHashMap<>();

  public <T> T getFieldValue(Object o, String fieldName) {
    return getFieldValue(o, o.getClass(), fieldName);
  }

  @SuppressWarnings("unchecked")
  public <T> T getFieldValue(Object o, Class<?> clazz, String fieldName) {
    try {
      Field field = getField(clazz, fieldName);
      return (T) field.get(o);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setFieldValue(Object o, String fieldName, Object value) {
    setFieldValue(o, o.getClass(), fieldName, value);
  }

  public void setFieldValue(Object o, Class<?> clazz, String fieldName, Object value) {
    try {
      Field field = getField(clazz, fieldName);
      field.set(o, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <T> T invokeMethod(Object o, String methodName, List<Object> parameters) {
    return invokeMethod(
        o,
        o.getClass(),
        methodName,
        parameters,
        parameters.stream().map(Object::getClass).collect(Collectors.toList()));
  }

  @SuppressWarnings("unchecked")
  public <T> T invokeMethod(
      Object o,
      Class<?> clazz,
      String methodName,
      List<Object> parameters,
      List<Class<?>> parameterClasses) {
    try {
      Method method = getMethod(clazz, methodName, parameterClasses);
      return (T) method.invoke(o, parameters.toArray());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Field getField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
    String uuid = clazz.getName() + "#" + fieldName;
    Field field = fieldCache.get(uuid);
    if (field != null) {
      return field;
    }

    field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    fieldCache.put(uuid, field);
    return field;
  }

  private Method getMethod(Class<?> clazz, String methodName, List<Class<?>> parameterClasses)
      throws NoSuchMethodException {
    String uuid =
        clazz.getName()
            + "#"
            + methodName
            + "#"
            + parameterClasses.stream()
                .map(c -> getClass().getName())
                .collect(Collectors.joining("#"));
    Method method = methodCache.get(uuid);
    if (method != null) {
      return method;
    }

    method = clazz.getDeclaredMethod(methodName, parameterClasses.toArray(new Class[0]));
    method.setAccessible(true);
    methodCache.put(uuid, method);
    return method;
  }
}
