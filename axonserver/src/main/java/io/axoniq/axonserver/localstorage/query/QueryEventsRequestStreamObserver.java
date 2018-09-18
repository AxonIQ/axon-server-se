package io.axoniq.axonserver.localstorage.query;

import io.axoniq.axondb.QueryValue;
import io.axoniq.axondb.grpc.ColumnsResponse;
import io.axoniq.axondb.grpc.Confirmation;
import io.axoniq.axondb.grpc.EventWithToken;
import io.axoniq.axondb.grpc.QueryEventsRequest;
import io.axoniq.axondb.grpc.QueryEventsResponse;
import io.axoniq.axondb.grpc.RowResponse;
import io.axoniq.axondb.query.EventStoreQueryParser;
import io.axoniq.axondb.query.Query;
import io.axoniq.axonserver.localstorage.EventStreamReader;
import io.axoniq.axonserver.localstorage.EventWriteStorage;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.query.result.AbstractMapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.BooleanExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.DefaultQueryResult;
import io.axoniq.axonserver.localstorage.query.result.EventExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.TimestampExpressionResult;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: marc
 */
public class QueryEventsRequestStreamObserver implements StreamObserver<QueryEventsRequest> {
    private static final Logger logger = LoggerFactory.getLogger(QueryEventsRequestStreamObserver.class);

    private static final ScheduledExecutorService senderService = Executors.newScheduledThreadPool(3,
                                                                                                   new CustomizableThreadFactory("ad-hoc-query-"));

    private final EventWriteStorage eventWriteStorage;
    private final EventStreamReader eventStreamReader;
    private final long defaultLimit;
    private final StreamObserver<QueryEventsResponse> responseObserver;
    private volatile Registration registration;
    private volatile Pipeline pipeLine;
    private volatile Sender sender;

    public QueryEventsRequestStreamObserver(EventWriteStorage eventWriteStorage, EventStreamReader eventStreamReader, long defaultLimit,
                                            StreamObserver<QueryEventsResponse> responseObserver) {
        this.eventWriteStorage = eventWriteStorage;
        this.eventStreamReader = eventStreamReader;
        this.defaultLimit = defaultLimit;
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(QueryEventsRequest queryEventsRequest) {
        try {
            if(sender == null) {
                sender = new Sender(queryEventsRequest.getNumberOfPermits(),
                                    queryEventsRequest.getLiveEvents(),
                                    System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30));
                long connectionToken = eventWriteStorage.getLastToken();
                long minConnectionToken = StringUtils.isEmpty(queryEventsRequest.getQuery()) ? Math.max(
                        connectionToken - defaultLimit, 0) : 0;
                String queryString = StringUtils.isEmpty(queryEventsRequest.getQuery()) ?
                        "token >= " + minConnectionToken + "| sortby(token)" : queryEventsRequest.getQuery();

                Query query = new EventStoreQueryParser().parse(queryString);
                query.addDefaultLimit(defaultLimit);
                logger.info("Executing query: {}", query);

                pipeLine = new QueryProcessor().buildPipeline(query, this::send);
                sendColumns(pipeLine);
                if (queryEventsRequest.getLiveEvents()) {
                    registration = eventWriteStorage.registerEventListener(event -> pushEventFromStream(event, pipeLine));
                }
                senderService.submit(() -> {
                    eventStreamReader.query(minConnectionToken,
                                            query.getStartTime(),
                                            event -> pushEvent(event, pipeLine));
                    sender.completed();
                });
            } else {
                sender.addPermits(queryEventsRequest.getNumberOfPermits());
            }

        } catch (ParseException pe) {
            responseObserver.onError(pe);
            sender = null;
        }
    }

    private void pushEventFromStream(EventWithToken event, Pipeline pipeLine) {
        logger.debug("Push event from stream");
        if( !pushEvent(event, pipeLine)) {
            logger.debug("Cancelling registation");
            registration.cancel();
        }
    }

    private boolean pushEvent(EventWithToken event, Pipeline pipeLine) {
        if (pipeLine == null) {
            return false;
        }
        try {
            return pipeLine.process(new DefaultQueryResult(new EventExpressionResult(event)));
        } catch (RuntimeException re) {
            responseObserver.onError(re);
            return false;
        }
    }

    private void sendColumns(Pipeline pipeLine) {
        sender.sendColumnNames(pipeLine.columnNames(EventExpressionResult.COLUMN_NAMES));
    }

    private boolean send(QueryResult exp) {
        return sender.send(exp.getId(), exp);
    }

    @Override
    public void onError(Throwable throwable) {
        logger.warn("Query stream cancelled with error", throwable);
        close();
    }

    @Override
    public void onCompleted() {
        close();
    }

    private void close() {
        if( registration != null) registration.cancel();
        pipeLine = null;
        if( sender != null) sender.stop();
    }

    private class Sender {
        private final boolean liveUpdates;
        private final long deadline;
        private final Map<Object, QueryResult> messages = new HashMap<>();
        private ScheduledFuture<?> sendTask;
        private final AtomicLong generatedId = new AtomicLong(0);
        private List<String> columns;
        private volatile QueryEventsResponse completeMessage;
        private final AtomicLong permits;

        public Sender( long permits, boolean liveUpdates, long deadline) {
            this.liveUpdates = liveUpdates;
            this.deadline = deadline;
            this.sendTask = senderService.scheduleWithFixedDelay(this::sendAll, 100, 100, TimeUnit.MILLISECONDS);
            this.permits = new AtomicLong(permits);
        }

        public void addPermits(long permits) {
            this.permits.addAndGet(permits);
        }

        public synchronized void stop() {
            if( sendTask != null && ! sendTask.isCancelled()) {
                sendTask.cancel(false);
                sendTask = null;
            }
        }

        public boolean send(Object identifyingValues, QueryResult result) {
            if(sendTask == null) return false;
            synchronized (messages) {
                if( messages.size() > 10000) {
                    logger.warn("Cancelling query as too many waiting results -{} results waiting", messages.size());
                    return false;
                }
                messages.put(identifyingValues == null ? generatedId.getAndIncrement(): identifyingValues, result);
            }
            return true;
        }


        private boolean deadlineExpired() {
            if( deadline < System.currentTimeMillis()) {
                logger.info("Cancelling query as deadline expired");
                responseObserver.onCompleted();
                stop();
                return true;
            }
            return false;
        }

        private boolean permitsLeft() {
            if( permits.get() <= 0) {
                logger.debug("No permits: {}", permits.get());
                return false;
            }
            return true;
        }

        public void sendAll() {
            if( deadlineExpired() || ! permitsLeft()) {
                return;
            }
            synchronized (messages) {
                Iterator<Map.Entry<Object, QueryResult>> it = messages.entrySet().iterator();
                while( it.hasNext() && permits.decrementAndGet() >= 0) {
                    sendToClient(responseObserver, it.next().getValue());
                    it.remove();
                }

                if( permits.get() >= 0 ) {
                    if( completeMessage != null) {
                        responseObserver.onNext(completeMessage);
                        completeMessage = null;
                        if( ! liveUpdates) {
                            responseObserver.onCompleted();
                            stop();
                        }
                    }
                } else {
                    permits.incrementAndGet();
                }
            }
        }

        private void sendToClient(StreamObserver<QueryEventsResponse> outputStream, QueryResult queryResult) {
            RowResponse.Builder rowBuilder = RowResponse.newBuilder();
            addIdValues(queryResult, rowBuilder);
            addSortValues(queryResult, rowBuilder);
            addDeleted(queryResult, rowBuilder);
            outputStream.onNext(QueryEventsResponse.newBuilder().setRow(rowBuilder).build());
        }

        private void addDeleted(QueryResult queryResult, RowResponse.Builder rowBuilder) {
            if( ! queryResult.isDeleted()) {
                if (queryResult.getValue() instanceof AbstractMapExpressionResult) {
                    AbstractMapExpressionResult abstractMapExpressionResult = (AbstractMapExpressionResult) queryResult.getValue();
                    columns.forEach(column -> {
                        try {
                            ExpressionResult value = abstractMapExpressionResult.getByIdentifier(column);
                            rowBuilder.putValues(column, wrap(value));
                        } catch (Exception ex) {
                            logger.warn("Failed to add results", ex);
                        }
                    });
                } else {
                    rowBuilder.putValues(columns.get(0), wrap(queryResult.getValue()));
                }
            }
        }

        private void addSortValues(QueryResult queryResult, RowResponse.Builder rowBuilder) {
            if( queryResult.getSortValues() != null) {
                queryResult.getSortValues().getValue().forEach(expressionResult -> {
                    if (expressionResult.isNonNull()) {
                        rowBuilder.addSortValues(wrap(expressionResult));
                    }
                });
            }
        }

        private void addIdValues(QueryResult queryResult, RowResponse.Builder rowBuilder) {
            if( queryResult.getId() != null) {
                queryResult.getId().getValue().forEach(expressionResult -> {
                    if (expressionResult.isNonNull()) {
                        rowBuilder.addIdValues(wrap(expressionResult));
                    }
                });
            }
        }

        private QueryValue wrap(ExpressionResult expressionResult) {
            if( expressionResult instanceof TimestampExpressionResult) {
                return QueryValue.newBuilder().setTextValue(expressionResult.toString()).build();
            }
            if( expressionResult instanceof NumericExpressionResult) {
                BigDecimal bd = expressionResult.getNumericValue();
                if( bd.scale() <= 0) return QueryValue.newBuilder().setNumberValue(expressionResult.getNumericValue().longValue()).build();
                return QueryValue.newBuilder().setDoubleValue(expressionResult.getNumericValue().doubleValue()).build();
            }
            if( expressionResult instanceof BooleanExpressionResult) {
                return QueryValue.newBuilder().setBooleanValue(expressionResult.isTrue()).build();
            }
            if( expressionResult instanceof MapExpressionResult) {
                return QueryValue.newBuilder().setTextValue(expressionResult.getValue().toString()).build();
            }

            return QueryValue.newBuilder().setTextValue(expressionResult.toString()).build();
        }

        public void sendColumnNames(List<String> columns) {
            this.columns = columns;
            responseObserver.onNext(QueryEventsResponse.newBuilder().setColumns(ColumnsResponse.newBuilder().addAllColumn(columns)).build());
        }

        public void completed() {
            this.completeMessage = QueryEventsResponse.newBuilder().setFilesCompleted(Confirmation.newBuilder().setSuccess(true)).build();
        }
    }

}
