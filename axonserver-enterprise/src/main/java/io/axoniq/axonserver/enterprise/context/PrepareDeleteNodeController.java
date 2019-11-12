package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.grpc.PlatformService;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

/**
 * @author Marc Gathier
 */
@Controller
public class PrepareDeleteNodeController {

    private final PlatformService platformService;
    private final String nodeName;

    public PrepareDeleteNodeController(MessagingPlatformConfiguration configuration, PlatformService platformService) {
        this.platformService = platformService;
        this.nodeName = configuration.getName();
    }

    @EventListener
    public void on(ContextEvents.DeleteNodeFromContextRequested prepareDeleteNodeFromContext) {
        if (nodeName.equals(prepareDeleteNodeFromContext.getNode())) {
            platformService.requestReconnectForContext(prepareDeleteNodeFromContext.getContext());
        }
    }
}
