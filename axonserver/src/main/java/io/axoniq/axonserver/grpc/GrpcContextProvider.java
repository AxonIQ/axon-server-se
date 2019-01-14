package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Controller;

/**
 * @author Marc Gathier
 */
@Controller
public class GrpcContextProvider implements ContextProvider {

    @Override
    public String getContext() {
        String context = GrpcMetadataKeys.CONTEXT_KEY.get();

        return context == null ? Topology.DEFAULT_CONTEXT : context;
    }
}
