package io.axoniq.axonserver;

import io.axoniq.axonserver.config.SystemInfoProvider;

import java.net.UnknownHostException;

/**
 * Author: marc
 */
public class TestSystemInfoProvider implements SystemInfoProvider {

    @Override
    public int getPort() {
        return 8024;
    }

    @Override
    public String getHostName() {
        return "localhost";
    }
}
