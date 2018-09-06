package io.axoniq.axonhub;

import io.axoniq.axonhub.config.SystemInfoProvider;

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
    public String getHostName() throws UnknownHostException {
        return "test";
    }
}
