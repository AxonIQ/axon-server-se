package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.context.ContextController;
import org.springframework.stereotype.Controller;

/**
 * Author: marc
 */
@Controller
public class GrpcContextProvider implements ContextProvider {

    @Override
    public String getContext() {
        String context = GrpcMetadataKeys.CONTEXT_KEY.get();

        return context == null ? ContextController.DEFAULT : context;
    }
}
