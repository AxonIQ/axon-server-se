package io.axoniq.axonserver.config;

import java.net.UnknownHostException;

/**
 * @author Marc Gathier
 */
public interface SystemInfoProvider {

    int getPort();

    String getHostName() throws UnknownHostException;
}
