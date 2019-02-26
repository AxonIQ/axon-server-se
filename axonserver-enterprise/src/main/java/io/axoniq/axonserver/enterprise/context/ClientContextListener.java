package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class ClientContextListener {

    private final PlatformService platformService;
    private final String nodeName;


    public ClientContextListener(PlatformService platformService, Topology topology) {
        this.platformService = platformService;
        this.nodeName = topology.getName();
    }


}
