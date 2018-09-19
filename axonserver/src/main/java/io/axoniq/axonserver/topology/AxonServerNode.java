package io.axoniq.axonserver.topology;

import java.util.Collection;

/**
 * Author: marc
 */
public interface AxonServerNode {

    String getHostName();

    Integer getGrpcPort();

    String getInternalHostName();

    Integer getGrpcInternalPort();

    Integer getHttpPort();

    String getName();

    Collection<String> getMessagingContextNames();

    Collection<String> getStorageContextNames();
}
