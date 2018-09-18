package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: marc
 */
@RestController("EventsRestController")
@RequestMapping("/v1/events")
public class EventsRestController {
    private final EventDispatcher eventStoreClient;
    private final Logger logger = LoggerFactory.getLogger(EventsRestController.class);

    public EventsRestController(EventDispatcher eventStoreClient) {
        this.eventStoreClient = eventStoreClient;
    }

    @GetMapping
    public SseEmitter listAggregateEvents(@RequestParam(value = "aggregateId") String aggregateId) {
        SseEmitter sseEmitter = new SseEmitter();
        GetAggregateEventsRequest request = GetAggregateEventsRequest.newBuilder()
                .setAggregateId(aggregateId)
                .setAllowSnapshots(true)
                .build();

            ObjectMapper objectMapper = new ObjectMapper();
            eventStoreClient.listAggregateEvents(request, new StreamObserver<InputStream>() {
                @Override
                public void onNext(InputStream event) {
                    try {
                        sseEmitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(new JsonEvent(Event.parseFrom(event)))));
                    } catch (Exception e) {
                        logger.debug("Exception on sending event - {}", e.getMessage(), e);                  }
                }

                @Override
                public void onError(Throwable throwable) {
                    sseEmitter.completeWithError(throwable);
                }

                @Override
                public void onCompleted() {
                    try {
                        sseEmitter.send(SseEmitter.event().comment("End of stream"));
                    } catch (IOException e) {
                        logger.debug("Error on sending completed", e);
                    }
                    sseEmitter.complete();
                }
            });
        return sseEmitter;
    }

    @KeepNames
    @JsonPropertyOrder({ "messageIdentifier", "aggregateIdentifier", "aggregateSequenceNumber", "aggregateType", "payloadType"
            , "payloadRevision" , "payload" , "timestamp" , "metaData" })
    private class JsonEvent {

        private final Map<String, Object> metaData = new HashMap<>();
        private final Event event;


        JsonEvent(Event event) {
            event.getMetaDataMap().forEach((key, value) -> {
                switch (value.getDataCase()) {
                    case TEXT_VALUE:
                        this.metaData.put(key, value.getTextValue());
                        break;
                    case NUMBER_VALUE:
                        this.metaData.put(key, value.getNumberValue());
                        break;
                    case BOOLEAN_VALUE:
                        this.metaData.put(key, value.getBooleanValue());
                        break;
                    case DOUBLE_VALUE:
                        this.metaData.put(key, value.getDoubleValue());
                        break;
                    case BYTES_VALUE:
                        this.metaData.put(key, value.getBytesValue());
                        break;
                    case DATA_NOT_SET:
                        break;
                }
            });
            this.event = event;
        }

        public Map<String, Object> getMetaData() {
            return metaData;
        }

        public String getPayload() {
            return event.getPayload().getData().toStringUtf8();
        }

        public String getAggregateType() {
            return event.getAggregateType();
        }

        public String getPayloadRevision() {
            return event.getPayload().getRevision();
        }
        public String getPayloadType() {
            return event.getPayload().getType();
        }

        public long getAggregateSequenceNumber() {
            return event.getAggregateSequenceNumber();
        }

        public String getAggregateIdentifier() {
            return event.getAggregateIdentifier();
        }

        public String getMessageIdentifier() {
            return event.getMessageIdentifier();
        }

        public long getTimestamp() {
            return event.getTimestamp();
        }
    }
}
