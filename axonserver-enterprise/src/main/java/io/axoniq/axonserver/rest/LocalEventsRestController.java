package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.rest.EventsRestController.JsonEvent;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.axonserver.AxonServerAccessController.CONTEXT_PARAM;
import static io.axoniq.axonserver.AxonServerAccessController.TOKEN_PARAM;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
@RestController
@RequestMapping("/v1/local")
public class LocalEventsRestController {

    private final LocalEventStore localEventStore;

    public LocalEventsRestController(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    @GetMapping(path = "snapshots")
    @ApiImplicitParams({
            @ApiImplicitParam(name = TOKEN_PARAM, value = "Access Token",
                    required = false, dataType = "string", paramType = "header")
    })
    public SseEmitter snapshots(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context,
            @RequestParam(value = "aggregateId", required = true) String aggregateId,
            @RequestParam(value = "maxSequence", defaultValue = "-1", required = false) long maxSequence,
            @RequestParam(value = "initialSequence", defaultValue = "0", required = false) long initialSequence) {
        SseEmitter sseEmitter = new SseEmitter();
        Function<InputStream, Object> mapping = new UncheckedFunction<>(input -> new JsonEvent(Event.parseFrom(input)));
        SSEEmitterStreamObserver<InputStream> streamObserver = new SSEEmitterStreamObserver<>(sseEmitter, mapping);
        GetAggregateSnapshotsRequest request = GetAggregateSnapshotsRequest.newBuilder()
                                                                           .setAggregateId(aggregateId)
                                                                           .setInitialSequence(initialSequence)
                                                                           .setMaxSequence(maxSequence >= 0? maxSequence : Long.MAX_VALUE)
                                                                           .build();
        localEventStore.listAggregateSnapshots(context, request, streamObserver);
        return sseEmitter;
    }

    @GetMapping(path = "events")
    @ApiImplicitParams({
            @ApiImplicitParam(name = TOKEN_PARAM, value = "Access Token",
                    required = false, dataType = "string", paramType = "header")
    })
    public SseEmitter events(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context,
            @RequestParam(value = "aggregateId", required = false) String aggregateId,
            @RequestParam(value = "initialSequence", defaultValue = "0", required = false) long initialSequence,
            @RequestParam(value = "allowSnapshots", defaultValue = "true", required = false) boolean allowSnapshots,
            @RequestParam(value = "trackingToken", defaultValue = "0", required = false) long trackingToken,
            @RequestParam(value = "timeout", defaultValue = "3600", required = false) long timeout) {
        SseEmitter sseEmitter = new SseEmitter(TimeUnit.SECONDS.toMillis(timeout));
        Function<InputStream, Object> mapping = new UncheckedFunction<>(input -> new JsonEvent(Event.parseFrom(input)));
        SSEEmitterStreamObserver<InputStream> streamObserver = new SSEEmitterStreamObserver<>(sseEmitter, mapping);
        if (aggregateId == null) {
            StreamObserver<GetEventsRequest> requestStream = localEventStore.listEvents(context, streamObserver);
            requestStream.onNext(GetEventsRequest.newBuilder()
                                                 .setTrackingToken(trackingToken)
                                                 .setNumberOfPermits(10000)
                                                 .setClientId("REST")
                                                 .build());
            sseEmitter.onCompletion(requestStream::onCompleted);
            sseEmitter.onTimeout(requestStream::onCompleted);
            sseEmitter.onError(requestStream::onError);
        } else {
            GetAggregateEventsRequest request = GetAggregateEventsRequest.newBuilder()
                                                                         .setAggregateId(aggregateId)
                                                                         .setAllowSnapshots(allowSnapshots)
                                                                         .setInitialSequence(initialSequence)
                                                                         .build();
            localEventStore.listAggregateEvents(context, request, streamObserver);
        }
        return sseEmitter;
    }

    private interface CheckedFunction<T, R> {

        R apply(T t) throws Exception;
    }

    private static class UncheckedFunction<T, R> implements Function<T, R> {

        private final Function<Exception, RuntimeException> mapping;
        private final CheckedFunction<T, R> delegate;

        public UncheckedFunction(CheckedFunction<T, R> delegate) {
            this(delegate, RuntimeException::new);
        }

        public UncheckedFunction(CheckedFunction<T, R> delegate, Function<Exception, RuntimeException> mapping) {
            this.delegate = delegate;
            this.mapping = mapping;
        }

        @Override
        public R apply(T t) {
            try {
                return delegate.apply(t);
            } catch (Exception e) {
                throw mapping.apply(e);
            }
        }
    }
}


