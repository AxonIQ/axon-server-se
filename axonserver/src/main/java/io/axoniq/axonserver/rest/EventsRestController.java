/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.rest.json.MetaDataJson;
import io.axoniq.axonserver.rest.json.SerializedObjectJson;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.ObjectUtils;
import io.axoniq.axonserver.util.StringUtils;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.AxonServerAccessController.CONTEXT_PARAM;
import static io.axoniq.axonserver.AxonServerAccessController.TOKEN_PARAM;
import static io.axoniq.axonserver.util.ObjectUtils.getOrDefault;

/**
 * REST endpoint to retrieve and store events and snapshots.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@RestController("EventsRestController")
@RequestMapping("/v1")
public class EventsRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final EventDispatcher eventStoreClient;
    private final Logger logger = LoggerFactory.getLogger(EventsRestController.class);

    public EventsRestController(EventDispatcher eventStoreClient) {
        this.eventStoreClient = eventStoreClient;
    }

    @GetMapping(path = "snapshots")
    @Parameters({
            @Parameter(name = TOKEN_PARAM, description = "Access Token", in = ParameterIn.HEADER)
    })
    public SseEmitter findSnapshots(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context,
            @RequestParam(value = "aggregateId", required = true) String aggregateId,
            @RequestParam(value = "maxSequence", defaultValue = "-1", required = false) long maxSequence,
            @RequestParam(value = "initialSequence", defaultValue = "0", required = false) long initialSequence,
            @Parameter(hidden = true) final Authentication principal) {
        auditLog.info("[{}@{}] Request for list of snapshots of aggregate \"{}\", [{}-{}]",
                      AuditLog.username(principal), context, aggregateId, initialSequence, maxSequence);

        SseEmitter sseEmitter = new SseEmitter();
        GetAggregateSnapshotsRequest request = GetAggregateSnapshotsRequest.newBuilder()
                                                                           .setAggregateId(aggregateId)
                                                                           .setInitialSequence(initialSequence)
                                                                           .setMaxSequence(maxSequence
                                                                                                   >= 0 ? maxSequence : Long.MAX_VALUE)
                                                                           .build();
        eventStoreClient.aggregateSnapshots(StringUtils.getOrDefault(context, Topology.DEFAULT_CONTEXT),
                                            getOrDefault(principal,
                                                         GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL),
                                            request)
                                .doOnError(sseEmitter::completeWithError)
                                .doOnComplete(() -> completeEmitter(sseEmitter))
                                .subscribe(event -> {
                                    try {
                                        sseEmitter.send(SseEmitter.event()
                                                                  .data(new JsonEvent(event.asEvent())));
                                    } catch (Exception e) {
                                        logger.debug("Exception on sending event - {}",
                                                     e.getMessage(),
                                                     e);
                                    }
                                });
        return sseEmitter;
    }


    @GetMapping(path = "events")
    @Parameters({
            @Parameter(name = TOKEN_PARAM, description = "Access Token", in = ParameterIn.HEADER)
    })
    public SseEmitter listAggregateEvents(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context,
            @RequestParam(value = "aggregateId", required = false) String aggregateId,
            @RequestParam(value = "initialSequence", defaultValue = "0", required = false) long initialSequence,
            @RequestParam(value = "allowSnapshots", defaultValue = "true", required = false) boolean allowSnapshots,
            @RequestParam(value = "trackingToken", defaultValue = "0", required = false) long trackingToken,
            @RequestParam(value = "timeout", defaultValue = "3600", required = false) long timeout,
            @RequestParam(value = "maxEvents", defaultValue = "10000", required = false) long maxEvents,
            @Parameter(hidden = true) final Authentication principal) {
        auditLog.info("[{}@{}] Request for an event-stream of aggregate \"{}\", starting at sequence {}, token {}.",
                      AuditLog.username(principal), context, aggregateId, initialSequence, trackingToken);

        SseEmitter sseEmitter = new SseEmitter(TimeUnit.SECONDS.toMillis(timeout));
        if (aggregateId != null) {

            GetAggregateEventsRequest request = GetAggregateEventsRequest.newBuilder()
                                                                         .setAggregateId(aggregateId)
                                                                         .setAllowSnapshots(allowSnapshots)
                                                                         .setInitialSequence(initialSequence)
                                                                         .build();

            ObjectMapper objectMapper = new ObjectMapper();
            Disposable subscription = eventStoreClient.aggregateEvents(context,
                                                                       getOrDefault(principal,
                                                                                    GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL),
                                                                       request)
                                                      .subscribe(event -> send(sseEmitter,
                                                                               objectMapper,
                                                                               event),
                                                                 sseEmitter::completeWithError,
                                                                 () -> completeEmitter(sseEmitter));
            handleSseEvent(sseEmitter, subscription::dispose);
        } else {
            GetEventsRequest getEventsRequest = GetEventsRequest.newBuilder()
                                                                .setTrackingToken(trackingToken)
                                                                .setNumberOfPermits(maxEvents)
                                                                .setClientId("REST")
                                                                .build();
            Sinks.Many<GetEventsRequest> requestsSink = Sinks.many().unicast().onBackpressureBuffer();
            eventStoreClient.events(context,
                                    getOrDefault(principal, GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL),
                                    requestsSink.asFlux())
                            .subscribe(serializedEventWithToken -> send(sseEmitter, serializedEventWithToken),
                                       sseEmitter::completeWithError,
                                       () -> completeEmitter(sseEmitter));
            requestsSink.emitNext(getEventsRequest, Sinks.EmitFailureHandler.FAIL_FAST);
            handleSseEvent(sseEmitter, () -> requestsSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST));
        }
        return sseEmitter;
    }

    private void handleSseEvent(SseEmitter sseEmitter, Runnable runnable) {
        sseEmitter.onError(e -> runnable.run());
        sseEmitter.onCompletion(runnable::run);
        sseEmitter.onTimeout(runnable::run);
    }

    private void send(SseEmitter sseEmitter, ObjectMapper objectMapper, SerializedEvent event) {
        try {
            sseEmitter.send(SseEmitter.event()
                                      .data(objectMapper
                                                    .writeValueAsString(new JsonEvent(event.asEvent()))));
        } catch (Exception e) {
            logger.debug("Exception on sending event - {}", e.getMessage(), e);
        }
    }

    private void send(SseEmitter sseEmitter, SerializedEventWithToken event) {
        try {
            sseEmitter.send(SseEmitter.event()
                                      .id(String.valueOf(event.getToken() + 1))
                                      .data(new JsonEvent(event.asEvent())));
        } catch (Exception e) {
            logger.debug("Exception on sending event - {}", e.getMessage(), e);
            sseEmitter.completeWithError(e);
        }
    }

    private void completeEmitter(SseEmitter emitter) {
        try {
            emitter.send(SseEmitter.event().comment("End of stream"));
        } catch (Exception e) {
            logger.debug("Error on sending completed", e);
        } finally {
            emitter.complete();
        }
    }

    @PostMapping("events")
    @Parameters({
            @Parameter(name = TOKEN_PARAM, description = "Access Token", in = ParameterIn.HEADER)
    })
    public Mono<Void> submitEvents(
            @RequestHeader(value = CONTEXT_PARAM, required = false, defaultValue = Topology.DEFAULT_CONTEXT) String context,
            @Valid @RequestBody JsonEventList jsonEvents,
            @Parameter(hidden = true) final Authentication principal) {
        if (jsonEvents.messages.isEmpty()) {
            throw new IllegalArgumentException("Missing messages");
        }

        Flux<SerializedEvent> events = Flux.fromStream(
                jsonEvents.messages
                        .stream()
                        .map(jsonEvent -> new SerializedEvent(jsonEvent.asEvent())));
        return eventStoreClient.appendEvent(context, principal, events);
    }

    /**
     * Store a new aggregate snapshot.
     *
     * @param context   the context where to add the snapshot
     * @param jsonEvent the snapshot data
     * @return completable future that completes when snapshot is stored.
     * @deprecated Use /v1/snapshots instead.
     */
    @PostMapping("snapshot")
    @Parameters({
            @Parameter(name = TOKEN_PARAM, description = "Access Token", in = ParameterIn.HEADER)
    })
    @Deprecated
    public Mono<Void> appendSnapshotOld(
            @RequestHeader(value = CONTEXT_PARAM, required = false, defaultValue = Topology.DEFAULT_CONTEXT) String context,
            @RequestBody @Valid JsonEvent jsonEvent,
            @Parameter(hidden = true) final Authentication principal) {
        auditLog.warn("[{}@{}] Request to append event(s) using deprecated API", AuditLog.username(principal), context);

        return appendSnapshot(context, jsonEvent, principal);
    }

    /**
     * Store a new aggregate snapshot.
     *
     * @param context   the context where to add the snapshot
     * @param jsonEvent the snapshot data
     * @return completable future that completes when snapshot is stored.
     */
    @PostMapping("snapshots")
    @Parameters({
            @Parameter(name = TOKEN_PARAM, description = "Access Token", in = ParameterIn.HEADER)
    })
    public Mono<Void> appendSnapshot(
            @RequestHeader(value = CONTEXT_PARAM, required = false, defaultValue = Topology.DEFAULT_CONTEXT) String context,
            @RequestBody @Valid JsonEvent jsonEvent,
            @Parameter(hidden = true) final Authentication principal) {
        auditLog.info("[{}@{}] Request to append event(s)", AuditLog.username(principal), context);

        return eventStoreClient.appendSnapshot(StringUtils.getOrDefault(context, Topology.DEFAULT_CONTEXT),
                                               jsonEvent.asEvent(),
                                               ObjectUtils.getOrDefault(principal,
                                                                        GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL));
    }

    @JsonPropertyOrder({"messageIdentifier",
            "aggregateIdentifier",
            "aggregateSequenceNumber",
            "aggregateType",
            "payloadType",
            "payloadRevision",
            "payload",
            "timestamp",
            "metaData"})
    public static class JsonEvent {

        private MetaDataJson metaData = new MetaDataJson();
        private String messageIdentifier;
        private String aggregateIdentifier;
        private long aggregateSequenceNumber;
        private String aggregateType;
        private SerializedObjectJson payload;
        private long timestamp;

        public JsonEvent() {
        }

        JsonEvent(Event event) {
            messageIdentifier = event.getMessageIdentifier();
            aggregateIdentifier = event.getAggregateIdentifier();
            aggregateSequenceNumber = event.getAggregateSequenceNumber();
            aggregateType = event.getAggregateType();
            if (event.hasPayload()) {
                payload = new SerializedObjectJson(event.getPayload());
            }
            timestamp = event.getTimestamp();
            metaData = new MetaDataJson(event.getMetaDataMap());
        }

        public String getMessageIdentifier() {
            return messageIdentifier;
        }

        public void setMessageIdentifier(String messageIdentifier) {
            this.messageIdentifier = messageIdentifier;
        }

        public String getAggregateIdentifier() {
            return aggregateIdentifier;
        }

        public void setAggregateIdentifier(String aggregateIdentifier) {
            this.aggregateIdentifier = aggregateIdentifier;
        }

        public long getAggregateSequenceNumber() {
            return aggregateSequenceNumber;
        }

        public void setAggregateSequenceNumber(long aggregateSequenceNumber) {
            this.aggregateSequenceNumber = aggregateSequenceNumber;
        }

        public String getAggregateType() {
            return aggregateType;
        }

        public void setAggregateType(String aggregateType) {
            this.aggregateType = aggregateType;
        }

        public void setMetaData(MetaDataJson metaData) {
            this.metaData = metaData;
        }

        public MetaDataJson getMetaData() {
            return metaData;
        }

        public SerializedObjectJson getPayload() {
            return payload;
        }

        public void setPayload(SerializedObjectJson payload) {
            this.payload = payload;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public Event asEvent() {
            return Event.newBuilder()
                        .setMessageIdentifier(StringUtils.getOrDefault(messageIdentifier, UUID.randomUUID().toString()))
                        .setAggregateIdentifier(StringUtils.getOrDefault(aggregateIdentifier, ""))
                        .setAggregateType(StringUtils.getOrDefault(aggregateType, ""))
                        .setAggregateSequenceNumber(aggregateSequenceNumber)
                        .setPayload(payload.asSerializedObject())
                        .setTimestamp(timestamp)
                        .putAllMetaData(metaData.asMetaDataValueMap()).build();
        }
    }

    public static class JsonEventList {

        @Size(min = 1, message = "'messages' field cannot be empty")
        @NotNull(message = "'messages' field cannot be missing")
        private List<JsonEvent> messages = new ArrayList<>();

        public List<JsonEvent> getMessages() {
            return messages;
        }

        public void setMessages(List<JsonEvent> messages) {
            this.messages = messages;
        }
    }
}
