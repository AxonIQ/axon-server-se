/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ExceptionUtils;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.ColumnsResponse;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.QueryValue;
import io.axoniq.axonserver.grpc.event.RowResponse;
import io.axoniq.axonserver.localstorage.AggregateReader;
import io.axoniq.axonserver.localstorage.EventDecorator;
import io.axoniq.axonserver.localstorage.EventStreamReader;
import io.axoniq.axonserver.localstorage.EventWriteStorage;
import io.axoniq.axonserver.localstorage.QueryOptions;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SnapshotWriteStorage;
import io.axoniq.axonserver.localstorage.query.result.AbstractMapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.BooleanExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.DefaultQueryResult;
import io.axoniq.axonserver.localstorage.query.result.EventExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.MapExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.NumericExpressionResult;
import io.axoniq.axonserver.localstorage.query.result.TimestampExpressionResult;
import io.axoniq.axonserver.queryparser.EventStoreQueryParser;
import io.axoniq.axonserver.queryparser.FunctionExpr;
import io.axoniq.axonserver.queryparser.Numeric;
import io.axoniq.axonserver.queryparser.Query;
import io.axoniq.axonserver.queryparser.QueryElement;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class QueryEventsRequestStreamObserver implements StreamObserver<QueryEventsRequest> {

    public static final int TIME_WINDOW_FIELD = 100;
    public static final String TIME_WINDOW_CUSTOM = "custom";
    private static final Logger logger = LoggerFactory.getLogger(QueryEventsRequestStreamObserver.class);

    private static final ScheduledExecutorService senderService = Executors.newScheduledThreadPool(3,
                                                                                                   new CustomizableThreadFactory(
                                                                                                           "ad-hoc-query-"));
    public static final String COLUMN_NAME_TOKEN = "token";

    private final SnapshotWriteStorage snapshotWriteStorage;
    private final EventStreamReader snapshotStreamReader;

    private final EventWriteStorage eventWriteStorage;
    private final EventStreamReader eventStreamReader;

    private final AggregateReader aggregateReader;
    private final long defaultLimit;
    private final long timeout;
    private final EventDecorator eventDecorator;
    private final StreamObserver<QueryEventsResponse> responseObserver;
    private final AtomicReference<Sender> senderRef = new AtomicReference<>();
    private volatile Registration registration;
    private volatile Pipeline pipeLine;

    public QueryEventsRequestStreamObserver(EventWriteStorage eventWriteStorage, EventStreamReader eventStreamReader,
                                            AggregateReader aggregateReader,
                                            long defaultLimit, long timeout, EventDecorator eventDecorator,
                                            StreamObserver<QueryEventsResponse> responseObserver,
                                            SnapshotWriteStorage snapshotWriteStorage,
                                            EventStreamReader snapshotStreamReader) {
        this.eventWriteStorage = eventWriteStorage;
        this.eventStreamReader = eventStreamReader;
        this.aggregateReader = aggregateReader;
        this.defaultLimit = defaultLimit;
        this.timeout = timeout;
        this.eventDecorator = eventDecorator;
        this.responseObserver = responseObserver;
        this.snapshotWriteStorage = snapshotWriteStorage;
        this.snapshotStreamReader = snapshotStreamReader;

    }

    @Override
    public void onNext(QueryEventsRequest queryEventsRequest) {
        try {
            boolean querySnapshots = queryEventsRequest.getQuerySnapshots();

            EventStreamReader streamReader;

            if(querySnapshots) {
                streamReader = snapshotStreamReader;
            } else {
                streamReader = eventStreamReader;
            }

            Sender sender = senderRef.updateAndGet(s -> {
                if (s == null) {
                    return new Sender(queryEventsRequest.getNumberOfPermits(),
                                      queryEventsRequest.getLiveEvents(),
                                      System.currentTimeMillis() + timeout);
                }
                return s;
            });
            if (sender.start()) {
                long connectionToken = streamReader.getLastToken();
                long maxToken = Long.MAX_VALUE;
                long minConnectionToken = StringUtils.isEmpty(queryEventsRequest.getQuery()) ? Math.max(
                        connectionToken - defaultLimit, 0) : 0;
                String queryString = StringUtils.isEmpty(queryEventsRequest.getQuery()) ?
                        "token >= " + minConnectionToken + "| sortby(token)" : queryEventsRequest.getQuery();
                String timeWindow = getTimeWindow(queryEventsRequest);
                if (!TIME_WINDOW_CUSTOM.equals(timeWindow)) {
                    queryString = queryString + " | " + timeWindow;
                }

                Query query = new EventStoreQueryParser().parse(queryString);
                query.addDefaultLimit(defaultLimit);
                String aggregateIdentifier = null;
                for (int i = 0; i < query.size(); i++) {
                    if (query.get(i) instanceof FunctionExpr) {
                        FunctionExpr functionExpr = (FunctionExpr) query.get(i);
                        if (isProjectOperation(functionExpr.operator())) {
                            break;
                        }
                        if (COLUMN_NAME_TOKEN.equals(functionExpr.getParameters().get(0).getLiteral()) &&
                                operatorIn(functionExpr.operator(), "=", ">", ">=")) {
                            minConnectionToken = getValueAsLong(minConnectionToken,
                                                                functionExpr.getParameters().get(1));
                        }
                        if (COLUMN_NAME_TOKEN.equals(functionExpr.getParameters().get(0).getLiteral()) &&
                                operatorIn(functionExpr.operator(), "<", "<=", "=")) {
                            maxToken = getValueAsLong(maxToken, functionExpr.getParameters().get(1));
                        }
                        if ("aggregateIdentifier".equals(functionExpr.getParameters().get(0).getLiteral()) &&
                                "=".equals(functionExpr.operator())) {
                            aggregateIdentifier = functionExpr.getParameters().get(1).getLiteral();
                        }
                    }
                }
                logger.info("Executing query: {}", query);
                pipeLine = new QueryProcessor().buildPipeline(query, this::send);
                sendColumns(pipeLine, aggregateIdentifier != null);
                if (queryEventsRequest.getLiveEvents() && maxToken > connectionToken) {
                    if(querySnapshots) {
                        registration = snapshotWriteStorage.registerEventListener((token, event) -> pushEventFromStream(
                                token,
                                Collections.singletonList(Event.newBuilder(event).setSnapshot(true).build()),
                                pipeLine));
                    } else {
                        registration = eventWriteStorage.registerEventListener((token, events) -> pushEventFromStream(token,
                                events,
                                pipeLine));
                    }
                }
                if (aggregateIdentifier == null) {
                    QueryOptions queryOptions = new QueryOptions(minConnectionToken, maxToken, query.getStartTime());
                    senderService.submit(() -> {
                        streamReader.query(queryOptions,
                                                event -> pushEvent(event, pipeLine));
                        Optional.ofNullable(senderRef.get())
                                .ifPresent(Sender::completed);
                    });
                } else {
                    String finalAggregateIdentifier = aggregateIdentifier;
                    senderService.submit(() -> {

                        Consumer<SerializedEvent> eventConsumer = serializedEvent -> {
                            Event event = serializedEvent.asEvent();
                            if (event.getTimestamp() >= query.getStartTime()) {
                                pushEvent(EventWithToken.newBuilder()
                                        .setEvent(event)
                                        .setToken(event.getAggregateSequenceNumber())
                                        .build(), pipeLine);
                            }
                        };

                        if (querySnapshots) {
                            aggregateReader.readSnapshots(finalAggregateIdentifier,
                                    0,
                                    Long.MAX_VALUE,
                                    Integer.MAX_VALUE,
                                    eventConsumer);
                        } else {
                            aggregateReader.readEvents(finalAggregateIdentifier,
                                    false,
                                    0, eventConsumer);
                        }


                        Optional.ofNullable(senderRef.get())
                                .ifPresent(Sender::completed);
                    });
                }
            } else {
                Optional.ofNullable(senderRef.get())
                        .ifPresent(s -> s.addPermits(queryEventsRequest.getNumberOfPermits()));
            }
        } catch (Exception pe) {
            responseObserver.onError(pe);
            senderRef.set(null);
        }
    }

    private long getValueAsLong(long maxToken, QueryElement value) {
        if (value instanceof Numeric) {
            try {
                maxToken = Long.parseLong(value.getLiteral());
            } catch (NumberFormatException ignore) {
                // ignore exception as this code is for optimization only
            }
        }
        return maxToken;
    }

    private boolean operatorIn(String operator, String... choices) {
        for (String choice : choices) {
            if (choice.equals(operator)) {
                return true;
            }
        }
        return false;
    }

    private boolean isProjectOperation(String operator) {
        return "count".equals(operator)
                || "min".equals(operator)
                || "max".equals(operator)
                || "avg".equals(operator)
                || "sum".equals(operator)
                || "select".equals(operator)
                || "groupby".equals(operator);
    }

    private String getTimeWindow(QueryEventsRequest queryEventsRequest) {
        List<ByteString> timeWindowList = queryEventsRequest.getUnknownFields().getField(TIME_WINDOW_FIELD)
                                                            .getLengthDelimitedList();
        if (timeWindowList.isEmpty()) {
            return TIME_WINDOW_CUSTOM;
        }

        return timeWindowList.get(0).toStringUtf8();
    }

    private void pushEventFromStream(long firstToken, List<Event> events, Pipeline pipeLine) {
        logger.debug("Push event from stream");
        for (Event event : events) {
            EventWithToken eventWithToken = EventWithToken.newBuilder().setEvent(event).setToken(firstToken++)
                                                          .build();
            try {
                if (!pushEvent(eventWithToken, pipeLine)) {
                    logger.debug("Cancelling registation");
                    cancelRegistration();
                    return;
                }
            } catch (RuntimeException re) {
                try {
                    cancelRegistration();
                    responseObserver.onError(re);
                } catch (Exception ex) {
                    //ignore
                }
            }
        }
    }

    private void cancelRegistration() {
        Optional.ofNullable(registration).ifPresent(Registration::cancel);
    }

    private boolean pushEvent(EventWithToken event, Pipeline pipeLine) {
        if (pipeLine == null) {
            return false;
        }
        try {
            return pipeLine.process(new DefaultQueryResult(new EventExpressionResult(eventDecorator
                                                                                             .decorateEventWithToken(
                                                                                                     event))));
        } catch (RuntimeException re) {
            try {
                cancelRegistration();
                responseObserver.onError(re);
            } catch (Exception ex) {
                //ignore
            }
            return false;
        }
    }

    private void sendColumns(Pipeline pipeLine, boolean hideToken) {
        Optional.ofNullable(senderRef.get())
                .ifPresent(sender -> {
                    if (hideToken) {
                        List<String> names = new ArrayList<>(pipeLine.columnNames(EventExpressionResult.COLUMN_NAMES));
                        names.remove("token");
                        sender.sendColumnNames(names);
                    } else {
                        sender.sendColumnNames(pipeLine.columnNames(EventExpressionResult.COLUMN_NAMES));
                    }
                });
    }

    private boolean send(QueryResult exp) {
        return Optional.ofNullable(senderRef.get())
                       .map(sender -> sender.send(exp.getId(), exp))
                       .orElse(false);
    }

    @Override
    public void onError(Throwable throwable) {
        if (!ExceptionUtils.isCancelled(throwable)) {
            logger.warn("Query stream cancelled with error", throwable);
        }
        close();
    }

    @Override
    public void onCompleted() {
        close();
    }

    private void close() {
        cancelRegistration();
        pipeLine = null;
        Optional.ofNullable(senderRef.get())
                .ifPresent(Sender::stop);
    }

    private class Sender {

        private final boolean liveUpdates;
        private final long deadline;
        private final Map<Object, QueryResult> messages = new HashMap<>();
        private final AtomicLong generatedId = new AtomicLong(0);
        private final AtomicLong permits;
        private final AtomicBoolean started = new AtomicBoolean(false);
        private ScheduledFuture<?> sendTask;
        private List<String> columns;
        private volatile QueryEventsResponse completeMessage;

        public Sender(long permits, boolean liveUpdates, long deadline) {
            this.liveUpdates = liveUpdates;
            this.deadline = deadline;
            this.permits = new AtomicLong(permits);
        }

        public void addPermits(long permits) {
            this.permits.addAndGet(permits);
        }

        public boolean start() {
            boolean notStarted = started.compareAndSet(false, true);
            if (notStarted) {
                this.sendTask = senderService.scheduleWithFixedDelay(this::sendAll, 100, 100, TimeUnit.MILLISECONDS);
            }
            return notStarted;
        }

        public synchronized void stop() {
            if (sendTask != null && !sendTask.isCancelled()) {
                sendTask.cancel(false);
                sendTask = null;
            }
        }

        public boolean send(Object identifyingValues, QueryResult result) {
            if (sendTask == null) {
                return false;
            }
            synchronized (messages) {
                if (messages.size() > 10000) {
                    throw new MessagingPlatformException(ErrorCode.TOO_MANY_EVENTS,
                                                         String.format(
                                                                 "Cancelling query as too many waiting results - %d results waiting",
                                                                 messages.size()));
                }
                messages.put(identifyingValues == null ? generatedId.getAndIncrement() : identifyingValues, result);
            }
            return true;
        }


        private boolean deadlineExpired() {
            if (deadline < System.currentTimeMillis()) {
                logger.info("Cancelling query as deadline expired");
                responseObserver.onCompleted();
                stop();
                return true;
            }
            return false;
        }

        private boolean permitsLeft() {
            if (permits.get() <= 0) {
                logger.debug("No permits: {}", permits.get());
                return false;
            }
            return true;
        }

        public void sendAll() {
            if (deadlineExpired() || !permitsLeft()) {
                return;
            }
            synchronized (messages) {
                Iterator<Map.Entry<Object, QueryResult>> it = messages.entrySet().iterator();
                while (it.hasNext() && permits.decrementAndGet() >= 0) {
                    sendToClient(responseObserver, it.next().getValue());
                    it.remove();
                }

                if (permits.get() >= 0) {
                    if (completeMessage != null) {
                        responseObserver.onNext(completeMessage);
                        completeMessage = null;
                        if (!liveUpdates) {
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
            if (!queryResult.isDeleted()) {
                if (queryResult.getValue() instanceof AbstractMapExpressionResult) {
                    AbstractMapExpressionResult abstractMapExpressionResult = (AbstractMapExpressionResult) queryResult
                            .getValue();
                    columns.forEach(column -> {
                        ExpressionResult value = abstractMapExpressionResult.getByIdentifier(column);
                        rowBuilder.putValues(column, wrap(value));
                    });
                } else {
                    rowBuilder.putValues(columns.get(0), wrap(queryResult.getValue()));
                }
            }
        }

        private void addSortValues(QueryResult queryResult, RowResponse.Builder rowBuilder) {
            if (queryResult.getSortValues() != null) {
                queryResult.getSortValues().getValue().forEach(expressionResult -> {
                    if (expressionResult.isNonNull()) {
                        rowBuilder.addSortValues(wrap(expressionResult));
                    }
                });
            }
        }

        private void addIdValues(QueryResult queryResult, RowResponse.Builder rowBuilder) {
            if (queryResult.getId() != null) {
                queryResult.getId().getValue().forEach(expressionResult -> {
                    if (expressionResult.isNonNull()) {
                        rowBuilder.addIdValues(wrap(expressionResult));
                    }
                });
            }
        }

        private QueryValue wrap(ExpressionResult expressionResult) {
            if (expressionResult == null || expressionResult.isNull()) {
                return QueryValue.getDefaultInstance();
            }
            if (expressionResult instanceof TimestampExpressionResult) {
                return QueryValue.newBuilder().setTextValue(expressionResult.toString()).build();
            }
            if (expressionResult instanceof NumericExpressionResult) {
                BigDecimal bd = expressionResult.getNumericValue();
                if (bd.scale() <= 0) {
                    return QueryValue.newBuilder().setNumberValue(expressionResult.getNumericValue()
                                                                                  .longValue())
                                     .build();
                }
                return QueryValue.newBuilder().setDoubleValue(expressionResult.getNumericValue().doubleValue()).build();
            }
            if (expressionResult instanceof BooleanExpressionResult) {
                return QueryValue.newBuilder().setBooleanValue(expressionResult.isTrue()).build();
            }
            if (expressionResult instanceof MapExpressionResult) {
                return QueryValue.newBuilder().setTextValue(expressionResult.getValue().toString()).build();
            }

            return QueryValue.newBuilder().setTextValue(expressionResult.toString()).build();
        }

        public void sendColumnNames(List<String> columns) {
            this.columns = columns;
            responseObserver.onNext(QueryEventsResponse.newBuilder()
                                                       .setColumns(ColumnsResponse.newBuilder().addAllColumn(columns))
                                                       .build());
        }

        public void completed() {
            this.completeMessage = QueryEventsResponse.newBuilder().setFilesCompleted(Confirmation.newBuilder()
                                                                                                  .setSuccess(true))
                                                      .build();
        }
    }
}
