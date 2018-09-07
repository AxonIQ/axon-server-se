package io.axoniq.axonserver.config;

import java.net.UnknownHostException;

/**
 * Author: marc
 */
public interface SystemInfoProvider {

    int getPort();

    String getHostName() throws UnknownHostException;
}
