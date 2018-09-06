package io.axoniq.sample;

import org.axonframework.queryhandling.GenericSubscriptionQueryUpdateMessage;
import org.axonframework.queryhandling.QueryUpdateEmitter;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.function.Predicate;

/**
 * Created by Sara Pellegrini on 26/06/2018.
 * sara.pellegrini@gmail.com
 */
@RestController
@RequestMapping("/update")
public class UpdateController {

    private final QueryUpdateEmitter emitter;

    public UpdateController(QueryUpdateEmitter emitter) {
        this.emitter = emitter;
    }

    @GetMapping("emit")
    public void emit(@RequestParam(value="value") int payload,
                     @RequestParam(value = "subscriptionId", required = false) String subscriptionId){
        Predicate<SubscriptionQueryMessage<?, ?, Integer>> predicate = subscription -> {
            boolean typeMatch = subscription.getUpdateResponseType().matches(int.class);
            boolean idMatch = subscriptionId == null || subscription.getIdentifier().equals(subscriptionId);
            return typeMatch && idMatch;
        };
        GenericSubscriptionQueryUpdateMessage<Integer> updateMessage = new GenericSubscriptionQueryUpdateMessage<>(payload);
        emitter.emit(predicate, updateMessage);
    }

    @GetMapping("updateComplete")
    public void updateComplete(@RequestParam(value = "subscriptionId", required = false) String id){
        emitter.complete(sqm -> id == null || sqm.getIdentifier().equals(id));
    }

    @GetMapping("updateCompleteExceptionally")
    public void updateCompleteExceptionally(@RequestParam(value = "subscriptionId", required = false) String id){
        emitter.completeExceptionally(sqm -> id == null || sqm.getIdentifier().equals(id),
                                      new RuntimeException("Error"));
    }
}
