package io.axoniq.axonserver.topology;

import java.util.Arrays;
import java.util.Collection;

/**
 * Author: marc
 */
public interface AxonServerNode {

    String getHostName();

    String getGrpcPort();

    String getInternalHostName();

    String getGrpcInternalPort();

    String getHttpPort();

    String getName();

    Collection<String> getMessagingContextNames();

    Collection<String> getStorageContextNames();
}
