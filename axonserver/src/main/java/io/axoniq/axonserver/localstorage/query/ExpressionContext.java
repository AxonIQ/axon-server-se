package io.axoniq.axonserver.localstorage.query;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Author: marc
 */
public class ExpressionContext {

    private final Map<String, Object> contextData = new ConcurrentHashMap<>();
    private final Map<Object, ExpressionContext> subContexts = new ConcurrentHashMap<>();

    public <T> T get(String key) {
        return (T) contextData.get(key);
    }

    public <T> T put(String key, T value) {
        return (T) contextData.put(key, value);
    }

    public <T> T computeIfAbsent(String key, Supplier<T> ifAbsent) {
        return (T) contextData.computeIfAbsent(key, k -> ifAbsent.get());
    }

    public ExpressionContext scoped(Object scope) {
        return subContexts.computeIfAbsent(scope, e -> new ExpressionContext());
    }

}
