package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.grpc.event.ColumnsResponse;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.QueryValue;
import io.axoniq.axonserver.grpc.event.RowResponse;
import io.axoniq.axonserver.message.event.EventStore;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.grpc.stub.StreamObserver;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Marc Gathier
 */
@Component
public class HttpStreamingQuery {
    private final Logger logger = LoggerFactory.getLogger(HttpStreamingQuery.class);
    private final ConcurrentMap<String, Sender> senderPerClient = new ConcurrentHashMap<>();
    private final EventStoreLocator eventStoreManager;

    public HttpStreamingQuery(EventStoreLocator eventStoreManager) {
        this.eventStoreManager = eventStoreManager;
    }

    @Async
    public void query(String context, String queryString, String clientToken, SseEmitter sseEmitter) {
        Sender oldSender = senderPerClient.remove(clientToken);
        if( oldSender != null) {
            logger.debug("Stopping sender for {}", clientToken);
            oldSender.stop();
        }
        try {
            EventStore eventStore =eventStoreManager.getEventStore(context);
            if( eventStore == null) {
                sseEmitter.send(SseEmitter.event().name("error").data("No Master for context: " + context));
                sseEmitter.complete();
                return;
            }
            Sender sender = new Sender(sseEmitter, eventStore, context, queryString);
            senderPerClient.put( clientToken, sender);
            sseEmitter.onTimeout(sender::stop);
        } catch (Exception e) {
            try {
                logger.warn("Error while processing query {} - {}", queryString, e.getMessage(), e);
                sseEmitter.send(SseEmitter.event().name("error").data(e.getMessage()));
            } catch (IOException ignore) {
                // ignore exception on sending error to client
            }
            sseEmitter.complete();
        }
    }

    private class Sender {
        private final SseEmitter sseEmitter;
        private final StreamObserver<QueryEventsRequest> querySender;

        public Sender( SseEmitter sseEmitter, EventStore eventStore, String context, String query) {
            this.sseEmitter = sseEmitter;
            this.querySender = eventStore.queryEvents(context, new StreamObserver<QueryEventsResponse>() {
                @Override
                public void onNext(QueryEventsResponse queryEventsResponse) {
                    try {
                        switch (queryEventsResponse.getDataCase()) {
                            case COLUMNS:
                                emitColumns(queryEventsResponse.getColumns());
                                break;
                            case ROW:
                                emitRows(queryEventsResponse.getRow());
                                break;
                            case FILES_COMPLETED:
                                emitCompleted();
                                break;
                            case DATA_NOT_SET:
                                break;
                        }
                    } catch (Exception exception) {
                        logger.warn("Failed to write to emitter", exception);
                        stop();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    try {
                        logger.warn("Error while processing query {} - {}", query, throwable.getMessage(), throwable);
                        sseEmitter.send(SseEmitter.event().name("error").data(throwable.getMessage()));
                    } catch (IOException ignore) {
                        // ignore exception on sending error to client
                    }
                    sseEmitter.complete();
                }

                @Override
                public void onCompleted() {
                    sseEmitter.complete();
                }
            });
            querySender.onNext(QueryEventsRequest.newBuilder().setLiveEvents(true).setNumberOfPermits(Long.MAX_VALUE).setQuery(query).build());
        }

        private void emitCompleted() throws IOException {
            sseEmitter.send(SseEmitter.event().name("done").data("Done"));
        }

        private void emitRows(RowResponse row) throws IOException, JSONException {
            JSONObject jsonObject = new JSONObject();
            if( row.getIdValuesCount() > 0) {
                JSONArray array = new JSONArray();
                row.getIdValuesList().forEach(qv -> addToArray(qv, array));
                jsonObject.put("idValues", array);
            }

            if (row.getSortValuesCount() > 0) {
                JSONArray array = new JSONArray();
                row.getSortValuesList().forEach(qv -> addToArray(qv, array));
                jsonObject.put("sortValues", array);
            }

            if( row.getValuesCount() > 0 ) {
                JSONObject values = new JSONObject();
                row.getValuesMap().forEach((key, qv) -> addToObject(values, key, qv));
                jsonObject.put("value", values);
            }
            //SampleCommandHandler
            sseEmitter.send(SseEmitter.event().name("row").data(jsonObject.toString()));
        }

        private void addToObject(JSONObject values, String key, QueryValue qv) {
            try {
            switch (qv.getDataCase()) {
                case TEXT_VALUE:
                    values.put(key, qv.getTextValue());
                    break;
                case NUMBER_VALUE:
                    values.put(key, qv.getNumberValue());
                    break;
                case BOOLEAN_VALUE:
                    values.put(key, qv.getBooleanValue());
                    break;
                case DOUBLE_VALUE:
                    values.put(key, qv.getDoubleValue());
                    break;
                case DATA_NOT_SET:
                    break;
            }
            } catch (JSONException e) {
                logger.debug("Failed to add value to JSON object", e);
            }
        }

        private void addToArray(QueryValue qv, JSONArray array)  {
            switch (qv.getDataCase()) {
                case TEXT_VALUE:
                    array.put(qv.getTextValue());
                    break;
                case NUMBER_VALUE:
                    array.put(qv.getNumberValue());
                    break;
                case BOOLEAN_VALUE:
                    array.put(qv.getBooleanValue());
                    break;
                case DOUBLE_VALUE:
                    try {
                        array.put(qv.getDoubleValue());
                    } catch (JSONException e) {
                        logger.debug("Failed to add value {} to JSON array", qv.getDoubleValue(), e);
                    }
                    break;
                case DATA_NOT_SET:
                    break;
            }
        }

        private void emitColumns(ColumnsResponse columns) throws IOException {
            JSONArray array = new JSONArray(columns.getColumnList());
            sseEmitter.send(SseEmitter.event().name("metadata").data(array.toString()));
        }

        public synchronized void stop() {
            querySender.onCompleted();
        }
    }
}
