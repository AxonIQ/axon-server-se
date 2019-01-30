package io.axoniq.axonserver.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Author: marc
 */
public interface SystemInfoProvider {

    default int getPort() {
        return 8080;
    }

    default String getHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    default boolean javaOnWindows() {
        String os = System.getProperty("os.name").toLowerCase();
        return os.startsWith("win");
    }

    default boolean javaWithModules() {
        String version = System.getProperty("java.version");
        return ! version.startsWith("1.8");
    }

}
