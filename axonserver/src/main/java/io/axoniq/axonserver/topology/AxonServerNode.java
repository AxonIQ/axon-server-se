package io.axoniq.axonserver.topology;

import java.util.Collection;

/**
 * @author Marc Gathier
 */
public interface AxonServerNode {

    String getHostName();

    Integer getGrpcPort();

    String getInternalHostName();

    Integer getGrpcInternalPort();

    Integer getHttpPort();

    String getName();

    Collection<String> getContextNames();

    default Collection<String> getStorageContextNames() {
        return getContextNames();
    }
}
