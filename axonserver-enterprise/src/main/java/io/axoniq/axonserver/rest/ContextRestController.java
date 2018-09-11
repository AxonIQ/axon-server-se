package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.licensing.Limits;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * Author: marc
 */
@RestController
@CrossOrigin
@RequestMapping("/v1/context")
public class ContextRestController {

    private final ContextController contextController;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final Limits limits;

    public ContextRestController( ContextController contextController,
                                  ApplicationEventPublisher applicationEventPublisher,
                                  Limits limits) {
        this.contextController = contextController;
        this.applicationEventPublisher = applicationEventPublisher;
        this.limits = limits;
    }

    @DeleteMapping( path = "{name}")
    public void deleteContext(@PathVariable("name")  String name) {
        if( ContextController.DEFAULT.equals(name)) throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_DEFAULT, "Cannot delete default context");

        applicationEventPublisher.publishEvent(contextController.deleteContext(name, false));
    }

    @PostMapping(path = "{context}/{node}")
    public void addNodeToContext(@PathVariable("context") String name, @PathVariable("node") String node, @RequestParam(name="storage", defaultValue = "true") boolean storage,
                                 @RequestParam(name="messaging", defaultValue = "true") boolean messaging
                                 ) {
        applicationEventPublisher.publishEvent(contextController.addNodeToContext(name,
                                                                                    node,
                                                                                    storage,
                                                                                    messaging,
                                                                                    false));
    }

    @DeleteMapping(path = "{context}/{node}")
    public void deleteNodeFromContext(@PathVariable("context") String name, @PathVariable("node") String node){
        applicationEventPublisher.publishEvent(contextController.deleteNodeFromContext(name, node, false));
    }

    @PostMapping
    public void addContext(@RequestBody @Valid ContextJSON contextJson) {
        if(!limits.isAddContextAllowed()) throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED, "License does not allow creating contexts");
        applicationEventPublisher.publishEvent(contextController.addContext(contextJson.getContext(), contextJson.getNodes(), false));
    }

}
