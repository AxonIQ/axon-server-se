package io.axoniq.axonserver.extensions.interceptor;

import java.util.Map;
import java.util.Set;

/**
 * @author Marc Gathier
 */
public interface InterceptorContext {

    String context();

    String principal();

    Set<String> principalRoles();

    Map<String, String> principalMetaData();

    void registerCompensatingAction(Runnable compensatingAction);

    void addDetails(String key, Object value);

    Object getDetails(String key);
}
