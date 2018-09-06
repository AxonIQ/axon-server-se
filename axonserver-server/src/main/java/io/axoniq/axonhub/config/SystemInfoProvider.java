package io.axoniq.axonhub.config;

import java.net.UnknownHostException;

/**
 * Author: marc
 */
public interface SystemInfoProvider {

    int getPort();

    String getHostName() throws UnknownHostException;
}
