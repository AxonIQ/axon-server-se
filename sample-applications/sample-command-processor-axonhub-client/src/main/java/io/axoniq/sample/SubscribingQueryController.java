package io.axoniq.sample;

import org.axonframework.messaging.Message;
import org.axonframework.queryhandling.GenericSubscriptionQueryMessage;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.queryhandling.SubscriptionQueryBackpressure;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.axonframework.queryhandling.SubscriptionQueryUpdateMessage;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.axonframework.queryhandling.responsetypes.ResponseTypes.instanceOf;
import static reactor.core.publisher.FluxSink.OverflowStrategy.valueOf;

/**
 * Created by Sara Pellegrini on 26/06/2018.
 * sara.pellegrini@gmail.com
 */
@RestController
@RequestMapping("/subscriptionQuery")
public class SubscribingQueryController {

    private final QueryBus queryBus;

    private final Map<String, SubscriptionQueryResult<QueryResponseMessage<Integer>, SubscriptionQueryUpdateMessage<Integer>>> registrationMap = new ConcurrentHashMap<>();

    public SubscribingQueryController(QueryBus queryBus) {
        this.queryBus = queryBus;
    }

    @GetMapping(value = "subscribeAll")
    public Flux<String> subscribeAll() {
        return subscribeAll(String.class.getName());
    }

    @GetMapping("subscribe")
    public String subscribe(@RequestParam(value = "buffer", required = false, defaultValue = "5") int bufferSize,
                            @RequestParam(value = "overflow", required = false, defaultValue = "ERROR") String overflow) {
        SubscriptionQueryBackpressure backpressure = new SubscriptionQueryBackpressure(valueOf(overflow));
        SubscriptionQueryMessage<String, Integer, Integer> message = new GenericSubscriptionQueryMessage<>(
                "", String.class.getName(), instanceOf(int.class), instanceOf(int.class));
        SubscriptionQueryResult<QueryResponseMessage<Integer>, SubscriptionQueryUpdateMessage<Integer>> result =
                queryBus.subscriptionQuery(message, backpressure, bufferSize);
        registrationMap.put(message.getIdentifier(), result);
        return message.getIdentifier();
    }

    @GetMapping("subscribeFlux")
    public Flux<Integer> subscribeFlux(@RequestParam(value="subscriptionId") String id) {
        if (registrationMap.containsKey(id)){
            return registrationMap.get(id).updates().map(Message::getPayload);
        }
        throw new IllegalArgumentException("Wrong subscriptionId:" + id);
    }

    @GetMapping("subscribeMono")
    public Mono<Integer> subscribeMono(@RequestParam(value="id") String id) {
        if (registrationMap.containsKey(id)){
            return registrationMap.get(id).initialResult().map(Message::getPayload);
        }
        throw new IllegalArgumentException("Wrong subscriptionId:" + id);
    }

    @GetMapping("unsubscribe")
    public void unsubscribe(@RequestParam(value="id") String id){
        if (registrationMap.containsKey(id)){
            registrationMap.remove(id).cancel();
        }
        throw new IllegalArgumentException("Wrong subscriptionId:" + id);
    }

    @GetMapping("noHandlers")
    public Flux<String> subscribeNoHandlers(){
        return subscribeAll("NotExistingQuery");
    }

    private Flux<String> subscribeAll(String queryName) {
        SubscriptionQueryMessage<String, Integer, Integer> message = new GenericSubscriptionQueryMessage<>(
                "", queryName, instanceOf(int.class), instanceOf(int.class));
        SubscriptionQueryResult<QueryResponseMessage<Integer>, SubscriptionQueryUpdateMessage<Integer>> result =
                queryBus.subscriptionQuery(message);
        return Flux.create(emitter -> {
            emitter.onDispose(result::cancel);
            emitter.next("Subscription started with identifier " + message.getIdentifier() + "<br>");
            result.initialResult().subscribe(r -> emitter.next("Initial result received with value: " + r.getPayload() + "<br>"),
                                             e -> emitter.complete()
            );
            result.updates().subscribe(u -> emitter.next("Update received with value: " + u.getPayload() + "<br>"),
                                       e -> emitter.complete());
        });
    }
}
