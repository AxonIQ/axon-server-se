package io.axoniq.axonserver.config;

import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultSystemInfoProvider implements SystemInfoProvider {
    private final Environment environment;

    public DefaultSystemInfoProvider(Environment environment) {
        this.environment = environment;
    }

    @Override
    public int getPort() {
        int httpPort;
        String portS = environment.getProperty("server.port", environment.getProperty("internal.server.port", "8080"));
        try {
            httpPort = Integer.valueOf(portS);
        } catch (NumberFormatException nfe) {
            httpPort = 8080;
        }
        return httpPort;
    }

    @Override
    public String getHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
