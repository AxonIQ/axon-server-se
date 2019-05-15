package io.axoniq.axonserver.enterprise.storage.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.protobuf.ByteString;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.NoOpEventTransformer;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import io.axoniq.axonserver.localstorage.transformation.WrappedEvent;
import io.axoniq.axonserver.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static io.axoniq.axonserver.util.StringUtils.getOrDefault;

/**
 * Implementation of the {@link EventStorageEngine} that stores events in a relational database.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class SpannerEventStorageEngine implements EventStorageEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerEventStorageEngine.class);
    private static final byte VERSION = 0;

    private final String maxGlobalIndex;
    private final String deleteAllData;
    private final String insertEvent;
    private final String maxSeqnrForAggid;
    private final String readEvents;

    private final String readEventsForAggidWithinRangeDesc;
    private final String readEventsForAggidDesc;
    private final String readEventsForAggidAsc;
    private final String tokenAt;
    private final String minToken;

    private final AtomicLong nextToken = new AtomicLong(0);
    private final AtomicLong lastToken = new AtomicLong(-1);
    private final EventTypeContext eventTypeContext;
    private final StorageProperties storageProperties;
    private final Predicate<String> leaderProvider;
    private final Set<Runnable> closeListeners = new CopyOnWriteArraySet<>();
    private final MetaDataSerializer metaDataSerializer;
    private volatile DatabaseClient dbClient;


    /**
     * Constructs the storage engine
     *
     * @param eventTypeContext     the combination of type (event/snapshot) and context for this engine
     * @param metaDataSerializer
     */
    public SpannerEventStorageEngine(EventTypeContext eventTypeContext,
                                     StorageProperties storageProperties,
                                     MetaDataSerializer metaDataSerializer,
                                     Predicate<String> leaderProvider) {
        this.eventTypeContext = eventTypeContext;
        this.storageProperties = storageProperties;
        this.metaDataSerializer = metaDataSerializer;
        this.leaderProvider = leaderProvider;

        maxGlobalIndex = String.format("select max(global_index) from %s", tableName());
        deleteAllData = String.format("delete from %s where 1=1", tableName());
        insertEvent = String.format(
                "insert into %s(global_index, aggregate_identifier, event_identifier, meta_data, payload, payload_revision, payload_type, sequence_number, time_stamp, type) "
                        + "values (@globalIndex,@aggregateIdentifier,@eventIdentifier,@metaData,@payload,@payloadRevision,@payloadType,@sequenceNumber,@timeStamp,@type)",
                tableName());
        maxSeqnrForAggid = String.format("select max(sequence_number) from %s where aggregate_identifier = @aggregateIdentifier",
                                         tableName());
        readEvents = String.format("select * from %s where global_index >= @startToken order by global_index asc",
                                   tableName());

        readEventsForAggidWithinRangeDesc = String.format(
                "select * from %s where aggregate_identifier = @aggregateIdentifier and sequence_number >= @minSequenceNumber and sequence_number <= @maxSequenceNumber order by sequence_number desc",
                tableName());
        readEventsForAggidDesc = String.format(
                "select * from %s where aggregate_identifier = @aggregateIdentifier and sequence_number >= @minSequenceNumber order by sequence_number desc",
                tableName());
        readEventsForAggidAsc = String.format(
                "select * from %s where aggregate_identifier = ? and sequence_number >= @minSequenceNumber order by sequence_number asc",
                tableName());
        tokenAt = String.format("select min(global_index) from %s where time_stamp >= @timeStamp", tableName());
        minToken = String.format("select min(global_index) from %s", tableName());
    }

    @Override
    public EventTypeContext getType() {
        return eventTypeContext;
    }

    /**
     * Returns an iterator to iterate through the transactions in the store. As the transaction information is not
     * stored in
     * the database, each event will be returned as a transaction.
     *
     * @param firstToken first tracking token to include in the iterator
     * @param limitToken last tracking token to include in the iterator (exclusive)
     * @return iterator of transactions
     */
    @Override
    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        CloseableIterator<SerializedEventWithToken> globalIterator = getGlobalIterator(firstToken, limitToken);
        return new Iterator<SerializedTransactionWithToken>() {
            @Override
            public boolean hasNext() {
                boolean next = globalIterator.hasNext();
                if (!next) {
                    globalIterator.close();
                }
                return next;
            }

            @Override
            public SerializedTransactionWithToken next() {
                SerializedEventWithToken event = globalIterator.next();
                return new SerializedTransactionWithToken(event.getToken(),
                                                          VERSION,
                                                          Collections.singletonList(event.getSerializedEvent()));
            }
        };
    }

    @Override
    public void init(boolean validating) {
        SpannerOptions options = SpannerOptions.newBuilder()
//                                               .setCredentials(storageProperties.getCredentials())
                                               .setProjectId(storageProperties.getProjectId())
                                               .build();

        Spanner service = options.getService();
        Database database;
        DatabaseAdminClient adminClient = service.getDatabaseAdminClient();
        try {
            database = adminClient.getDatabase(storageProperties.getInstance(), eventTypeContext.getContext());
            LOGGER.warn("Database exists, checking tables");

        } catch (Exception ex) {
            try {
                database = adminClient.createDatabase(storageProperties.getInstance(), eventTypeContext.getContext(), initStatements()).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MessagingPlatformException(ErrorCode.INTERRUPTED, "Interrupted while creating spanner db");
            } catch (ExecutionException e) {
                LOGGER.warn("Failed to create spanner db {}", eventTypeContext.getContext(), e.getCause());
                throw new MessagingPlatformException(ErrorCode.OTHER, "Error while creating spanner db", e.getCause());
            }
        }
        DatabaseId db = DatabaseId.of(options.getProjectId(), storageProperties.getInstance(), eventTypeContext.getContext());
        this.dbClient = service.getDatabaseClient(db);
        createTableIfNotExists(adminClient);

        com.google.cloud.spanner.ResultSet resultSet = this.dbClient.singleUse().executeQuery(Statement.of(maxGlobalIndex));
        if( resultSet.next() && ! resultSet.isNull(0)) {
                lastToken.set(resultSet.getLong(0));
        }
        nextToken.set(lastToken.get()+1);

    }

    private void createTableIfNotExists(DatabaseAdminClient adminClient ) {
        ResultSet resultSet = dbClient.singleUse().executeQuery(Statement.of("select table_name, table_schema from information_schema.tables where table_name = '" + tableName()
                                                               + "'"));
        if( resultSet.next()) {
            LOGGER.warn("Found table in schema {}", resultSet.getString(1));
        } else {
            OperationFuture<Void, UpdateDatabaseDdlMetadata> operation = adminClient
                    .updateDatabaseDdl(storageProperties.getInstance(),
                                       eventTypeContext.getContext(),
                                       initStatements(),
                                       null);
            try {
                operation.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MessagingPlatformException(ErrorCode.INTERRUPTED, "Interrupted while creating spanner tables");
            } catch (ExecutionException e) {
                throw new MessagingPlatformException(ErrorCode.OTHER, "Error while creating spanner tables", e.getCause());
            }
        }

    }

    private Iterable<String> initStatements() {

        String tableName = tableName();
        String createTable = String.format(
                "create table %s ("
                        + "global_index int64 not null, "
                        + "aggregate_identifier string(255) not null, "
                        + "event_identifier string(255) not null, "
                        + "meta_data bytes(max), "
                        + "payload bytes(max) not null, "
                        + "payload_revision string(255), "
                        + "payload_type string(255) not null, "
                        + "sequence_number int64 not null, "
                        + "time_stamp int64 not null, "
                        + "type string(255)) "
                        + "primary key (global_index)", tableName);
        String createIndexAggidSeqnr = String.format(
                "create unique index %s_uk1 on %s (aggregate_identifier, sequence_number)", tableName, tableName);
        String createIndexEventId = String.format(
                "create unique index %s_uk2 on %s  (event_identifier)", tableName, tableName);

        return Arrays.asList(createTable, createIndexAggidSeqnr, createIndexEventId);
    }

    private String tableName() {
        return ("axon_" + eventTypeContext.getEventType()).toLowerCase();
    }

    @Override
    public void deleteAllEventData() {
        dbClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
            @Nullable
            @Override
            public Void run(TransactionContext transaction) throws Exception {
                transaction.executeUpdate(Statement.of(deleteAllData));
                return null;
            }
        });
    }



    private PreparedTransaction prepareTransaction(List<SerializedEvent> eventList) {
        long firstToken = nextToken.getAndAdd(eventList.size());
        return new PreparedTransaction(firstToken,
                                       eventList.stream().map(e -> new WrappedEvent(e, NoOpEventTransformer.INSTANCE))
                                                .collect(Collectors.toList()));
    }

    @Override
    public long nextToken() {
        return nextToken.get();
    }

    @Override
    public CompletableFuture<Long> store(List<SerializedEvent> eventList) {
        PreparedTransaction preparedTransaction = prepareTransaction(eventList);
        if (!leaderProvider.test(eventTypeContext.getContext())) {
            return CompletableFuture.completedFuture(preparedTransaction.getToken());
        }
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            dbClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
                @Nullable
                @Override
                public Void run(TransactionContext transaction) {

                    //                        + "values (@globalIndex,@aggregateIdentifier,@eventIdentifier,@metaData,@payload,@payloadRevision,
                    // @payloadType,@sequenceNumber,@timeStamp,@type)",
                    long firstToken = preparedTransaction.getToken();
                    List<Statement> statements = new ArrayList<>(preparedTransaction.getEventList().size());
                    for (ProcessedEvent event : preparedTransaction.getEventList()) {
                        statements.add(Statement.newBuilder(insertEvent)
                                                .bind("globalIndex").to(firstToken++)
                                                .bind("aggregateIdentifier").to(event.isDomainEvent() ?
                                                                                        event.getAggregateIdentifier() : event
                                        .getMessageIdentifier())
                                                .bind("eventIdentifier").to(event.getMessageIdentifier())
                                                .bind("metaData").to(ByteArray.copyFrom(metaDataSerializer.serialize(event.getMetaData())))
                                                .bind("payload").to(ByteArray.copyFrom(event.getPayloadBytes()))
                                                .bind("payloadRevision").to(event.getPayloadRevision())
                                                .bind("payloadType").to(event.getPayloadType())
                                                .bind("sequenceNumber").to(event.getAggregateSequenceNumber())
                                                .bind("timeStamp").to(event.getTimestamp())
                                                .bind("type").to(event.getAggregateType())
                                                .build());
                    }
                    long[] count = transaction.batchUpdate(statements);
                    LOGGER.warn("Records inserted {}", count);
                    lastToken.addAndGet(statements.size());
                    return null;
                }
            });
            completableFuture.complete(preparedTransaction.getToken());
        } catch( Exception ex) {
            completableFuture.completeExceptionally(ex);
        }
        return completableFuture;
    }

    @Override
    public long getLastToken() {
        return lastToken.get();
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, boolean checkAll) {
        Statement statement = Statement.newBuilder(maxSeqnrForAggid).bind("aggregateIdentifier").to(aggregateIdentifier).build();
        com.google.cloud.spanner.ResultSet resultSet = dbClient.singleUse().executeQuery(statement);
        if( resultSet.next() && ! resultSet.isNull(0)) {
            return Optional.of(resultSet.getLong(0));
        }
        return Optional.empty();
    }


    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
        return getGlobalIterator(start, Long.MAX_VALUE);
    }

    @Override
    public void close() {
        closeListeners.forEach(Runnable::run);
    }

    @Override
    public Registration registerCloseListener(Runnable listener) {
        closeListeners.add(listener);
        return () -> closeListeners.remove(listener);
    }

    private CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start, long end) {
        Statement statement = Statement.newBuilder(readEvents).bind("startToken").to(start)
                                       .build();
        AtomicReference<ResultSet> resultSet = new AtomicReference<>(dbClient.singleUse()
                                                       .executeQuery(statement));

            return new CloseableIterator<SerializedEventWithToken>() {
                long nextIndex = start;
                boolean hasNext = start < end && resultSet.get().next();

                @Override
                public void close() {
                    close(true);
                }

                private void close(boolean disconnect) {
                    resultSet.get().close();
                }

                @Override
                public boolean hasNext() {
                    if (!hasNext && nextIndex < end && nextIndex <= getLastToken()) {
                            LOGGER.debug("Create new query at {}", nextIndex);
                            Statement statement = Statement.newBuilder(readEvents).bind("startToken").to(nextIndex)
                                                           .build();
                            resultSet.set(dbClient.singleUse().executeQuery(statement));
                            hasNext = resultSet.get().next();
                    }
                    return hasNext;
                }

                @Override
                public SerializedEventWithToken next() {
                    if (!hasNext) {
                        throw new NoSuchElementException("No more elements");
                    }

                        SerializedEventWithToken event = readEventWithToken(resultSet.get());
                        nextIndex = event.getToken() + 1;
                        hasNext = nextIndex < end && resultSet.get().next();
                        if (!hasNext) {
                            close(false);
                        }
                        return event;
                }
            };
    }

    private SerializedEventWithToken readEventWithToken(com.google.cloud.spanner.ResultSet resultSet)  {
        return new SerializedEventWithToken(resultSet.getLong("global_index"),
                                            readEvent(resultSet));
    }

    private SerializedEvent readEvent(com.google.cloud.spanner.ResultSet resultSet)  {
        String type = resultSet.getString("type");
        Event.Builder builder = Event.newBuilder()
                                     .setAggregateSequenceNumber(resultSet.getLong("sequence_number"))
                                     .setMessageIdentifier(resultSet.getString("event_identifier"))
                                     .setPayload(SerializedObject.newBuilder()
                                                                 .setData(ByteString.copyFrom(resultSet.getBytes(
                                                                         "payload").toByteArray()))
                                                                 .setRevision(getOrDefault(resultSet.getString(
                                                                         "payload_revision"), ""))
                                                                 .setType(getOrDefault(resultSet
                                                                                               .getString("payload_type"),
                                                                                       "")))
                                     .putAllMetaData(metaDataSerializer.deserialize(resultSet.getBytes("meta_data").toByteArray()))
                                     .setTimestamp(resultSet.getLong("time_stamp"))
                                     .setAggregateType(type);

        if (!StringUtils.isEmpty(type)) {
            builder.setAggregateIdentifier(resultSet.getString("aggregate_identifier"));
        }
        return new SerializedEvent(builder.build());
    }

    @Override
    public Optional<SerializedEvent> getLastEvent(String aggregateIdentifier, long minSequenceNumber) {
        Statement statement = Statement.newBuilder(readEventsForAggidDesc).bind("aggregateIdentifier").to(aggregateIdentifier)
                                       .bind("minSequenceNumber").to(minSequenceNumber)
                                       .build();
        com.google.cloud.spanner.ResultSet resultSet = dbClient.singleUse()
                                                               .executeQuery(statement);
        if( resultSet.next()) {
            return Optional.of(readEvent(resultSet));
        }

        return Optional.empty();
    }

    @Override
    public void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber,
                                          long actualMaxSequenceNumber, int maxResults,
                                          Consumer<SerializedEvent> eventConsumer) {

        Statement statement = Statement.newBuilder(readEventsForAggidWithinRangeDesc + " limit " + maxResults).bind("aggregateIdentifier").to(aggregateId)
                .bind("minSequenceNumber").to(actualMinSequenceNumber)
                .bind("maxSequenceNumber").to(actualMaxSequenceNumber)
                .build();


        com.google.cloud.spanner.ResultSet resultSet = dbClient.singleUse()
                                                               .executeQuery(statement);
        while( resultSet.next()) {
                    eventConsumer.accept(readEvent(resultSet));
        }
    }

    @Override
    public void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber,
                                          Consumer<SerializedEvent> eventConsumer) {
        Statement statement = Statement.newBuilder(readEventsForAggidAsc).bind("aggregateIdentifier").to(aggregateId)
                                       .bind("minSequenceNumber").to(actualMinSequenceNumber)
                                       .build();

        com.google.cloud.spanner.ResultSet resultSet = dbClient.singleUse().executeQuery(statement);
        while( resultSet.next()) {
            eventConsumer.accept(readEvent(resultSet));
        }
    }


    @Override
    public long getFirstToken() {
        long min = 0;
        com.google.cloud.spanner.ResultSet resultSet = dbClient.singleUse()
                                                               .executeQuery(Statement.of(minToken));
        if( resultSet.next() && !resultSet.isNull(0)) {
            min = resultSet.getLong(0);
        }

        return min;
    }

    @Override
    public long getTokenAt(long instant) {
        long min = -1;
        Statement statement = Statement.newBuilder(tokenAt).bind("timeStamp").to(instant)
                                       .build();
        com.google.cloud.spanner.ResultSet resultSet = dbClient.singleUse()
                                                               .executeQuery(statement);
        if( resultSet.next() && !resultSet.isNull(0)) {
            min = resultSet.getLong(0);
        }
        return min;
    }

    @Override
    public void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer) {
        String query = String.format(
                "select * from %s where global_index >= @minToken and time_stamp >= @timeStamp order by global_index desc",
                tableName());
        Statement statement = Statement.newBuilder(query)
                                       .bind("timeStamp").to(minTimestamp)
                                       .bind("minToken").to(minToken)
                                       .build();
        com.google.cloud.spanner.ResultSet resultSet = dbClient.singleUse()
                                                               .executeQuery(statement);
        while (resultSet.next()) {
            SerializedEventWithToken serializedEventWithToken = readEventWithToken(resultSet);
            if (!consumer.test(serializedEventWithToken.asEventWithToken())) {
                return;
            }
        }
    }
}
