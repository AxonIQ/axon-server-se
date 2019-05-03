package io.axoniq.axonserver.enterprise.storage.jdbc;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.NoOpEventTransformer;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import io.axoniq.axonserver.localstorage.transformation.WrappedEvent;
import org.springframework.data.util.CloseableIterator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import static io.axoniq.axonserver.util.StringUtils.getOrDefault;

/**
 * @author Marc Gathier
 */
public abstract class JdbcAbstractStore implements EventStorageEngine {
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
    private final EventTypeContext eventTypeContext;
    private final DataSource dataSource;
    private final MetaDataSerializer metaDataSerializer;
    private final MultiContextStrategy multiContextStrategy;
    private final SyncStrategy syncStrategy;

    protected JdbcAbstractStore(EventTypeContext eventTypeContext,
                                DataSource dataSource,
                                MetaDataSerializer metaDataSerializer,
                                MultiContextStrategy multiContextStrategy,
                                SyncStrategy syncStrategy) {
        this.eventTypeContext = eventTypeContext;
        this.dataSource = dataSource;
        this.metaDataSerializer = metaDataSerializer;
        this.multiContextStrategy = multiContextStrategy;
        this.syncStrategy = syncStrategy;

        maxGlobalIndex = String.format("select max(global_index) from %s", getTableName());
        deleteAllData = String.format("delete from %s", getTableName());
        insertEvent = String.format("insert into %s(global_index, aggregate_identifier, event_identifier, meta_data, payload, payload_revision, payload_type, sequence_number, time_stamp, type) values (?,?,?,?,?,?,?,?,?,?)", getTableName());
        maxSeqnrForAggid = String.format("select max(sequence_number) from %s where aggregate_identifier = ?", getTableName());
        readEvents = String.format("select * from %s where global_index >= ? order by global_index asc", getTableName());

        readEventsForAggidWithinRangeDesc = String.format("select * from %s where aggregate_identifier = ? and sequence_number >= ? and sequence_number <= ? order by sequence_number desc", getTableName());
        readEventsForAggidDesc = String.format("select * from %s where aggregate_identifier = ? and sequence_number >= ? order by sequence_number desc", getTableName());
        readEventsForAggidAsc = String.format("select * from %s where aggregate_identifier = ? and sequence_number >= ? order by sequence_number asc", getTableName());
        tokenAt = String.format("select min(global_index) from %s where time_stamp >= ?", getTableName());
        minToken = String.format("select min(global_index) from %s", getTableName());

    }

    @Override
    public EventTypeContext getType() {
        return eventTypeContext;
    }

    @Override
    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        CloseableIterator<SerializedEventWithToken> globalIterator = getGlobalIterator(firstToken, limitToken);
        return new Iterator<SerializedTransactionWithToken>() {
            @Override
            public boolean hasNext() {
                return globalIterator.hasNext();
            }

            @Override
            public SerializedTransactionWithToken next() {
                SerializedEventWithToken event = globalIterator.next();
                return new SerializedTransactionWithToken(event.getToken(), VERSION, Collections.singletonList(event.getSerializedEvent()));
            }
        };
    }

    @Override
    public void init(boolean validating) {
        try (Connection connection = dataSource.getConnection()) {
            multiContextStrategy.init(eventTypeContext, connection);
            try (PreparedStatement preparedStatement = connection.prepareStatement(
                    maxGlobalIndex);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    Number last = (Number) resultSet.getObject(1);
                    if (last != null) nextToken.set(last.longValue()+1);
                }
            }
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, eventTypeContext + " " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteAllEventData() {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        deleteAllData);
            }
            nextToken.set(0);
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        }
    }


    @Override
    public PreparedTransaction prepareTransaction( List<SerializedEvent> eventList) {
        long firstToken = nextToken.getAndAdd(eventList.size());
        return new PreparedTransaction(firstToken, eventList.stream().map(e -> new WrappedEvent(e, NoOpEventTransformer.INSTANCE)).collect(Collectors.toList()));
    }

    @Override
    public long nextToken() {
        return nextToken.get();
    }

    @Override
    public CompletableFuture<Long> store(PreparedTransaction preparedTransaction) {
        if (!syncStrategy.storeOnNode(eventTypeContext)) {
            return CompletableFuture.completedFuture(preparedTransaction.getToken());
        }
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            long firstToken = preparedTransaction.getToken();
            try (PreparedStatement insert = connection.prepareStatement(
                    insertEvent)) {
                for (ProcessedEvent event : preparedTransaction.getEventList()) {
                    insert.setLong(1, firstToken++);
                    if( event.isDomainEvent()) {
                        insert.setString(2, event.getAggregateIdentifier());
                    } else {
                        insert.setString(2, event.getMessageIdentifier());
                    }
                    insert.setString(3, event.getMessageIdentifier());
                    insert.setBytes(4, metaDataSerializer.serialize(event.getMetaData()));
                    insert.setBytes(5, event.getPayloadBytes());
                    insert.setString(6, event.getPayloadRevision());
                    insert.setString(7, event.getPayloadType());
                    insert.setLong(8, event.getAggregateSequenceNumber());
                    insert.setLong(9, event.getTimestamp());
                    insert.setString(10, event.getAggregateType());
                    insert.execute();
                }
            }
            connection.commit();
            completableFuture.complete(preparedTransaction.getToken());
        } catch (SQLException e) {
            completableFuture.completeExceptionally(e);
        }
        return completableFuture;
    }

    @Override
    public long getLastToken() {
        return nextToken.get()-1;
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, boolean checkAll) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     maxSeqnrForAggid)) {
            preparedStatement.setString(1, aggregateIdentifier);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    Number last = (Number) resultSet.getObject(1);
                    if (last != null) return Optional.of(last.longValue());
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }


    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
        return getGlobalIterator(start, Long.MAX_VALUE);
    }

    private CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start, long end) {
        try {
            AtomicReference<Connection> connection = new AtomicReference<>(dataSource.getConnection());
            AtomicReference<PreparedStatement> preparedStatement = new AtomicReference<>(connection.get().prepareStatement(readEvents));
            preparedStatement.get().setLong(1, start);
            AtomicReference<ResultSet> resultSet = new AtomicReference<>(preparedStatement.get().executeQuery());

            return new CloseableIterator<SerializedEventWithToken>() {
                long nextIndex = start;
                boolean hasNext = start < end && resultSet.get().next();
                @Override
                public void close() {
                    try {
                        resultSet.get().close();
                        preparedStatement.get().close();
                        connection.get().close();
                    } catch (SQLException e) {
                        // Ignore exceptions on close
                    }
                }

                @Override
                public boolean hasNext() {
                    if( ! hasNext && nextIndex < end && nextIndex <= getLastToken()) {
                        try {
                        connection.set(dataSource.getConnection());
                        preparedStatement.set(connection.get().prepareStatement(readEvents));
                        preparedStatement.get().setLong(1, nextIndex);
                        resultSet.set( preparedStatement.get().executeQuery());
                        hasNext = resultSet.get().next();
                        } catch (SQLException e) {
                            // Ignore exceptions on close
                        }
                    }
                    return hasNext;
                }

                @Override
                public SerializedEventWithToken next() {
                    if( !hasNext) {
                        throw new NoSuchElementException("No more elements");
                    }

                    try {
                        SerializedEventWithToken event =  readEventWithToken(resultSet.get());
                        nextIndex = event.getToken() + 1;
                        hasNext = nextIndex < end && resultSet.get().next();
                        if( !hasNext) {
                            close();
                        }
                        return event;
                    } catch (SQLException e) {
                        throw new NoSuchElementException(e.getMessage());
                    }
                }
            };
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    private SerializedEventWithToken readEventWithToken(ResultSet resultSet) throws SQLException {
        return new SerializedEventWithToken(resultSet.getLong("global_index"),
                                            readEvent(resultSet));
    }

    private SerializedEvent readEvent(ResultSet resultSet) throws SQLException {
        String type =resultSet.getString("type");
        Event.Builder builder = Event.newBuilder()
                                     .setAggregateSequenceNumber(resultSet.getLong("sequence_number"))
                                     .setMessageIdentifier(resultSet.getString("event_identifier"))
                                     .setPayload(SerializedObject.newBuilder()
                                                                 .setData(ByteString.copyFrom(resultSet.getBytes(
                                                                         "payload")))
                                                                 .setRevision(getOrDefault(resultSet.getString(
                                                                         "payload_revision"), ""))
                                                                 .setType(getOrDefault(resultSet
                                                                                               .getString("payload_type"),
                                                                                       "")))
                                     .putAllMetaData(metaDataSerializer.deserialize(resultSet.getBytes("meta_data")))
                                     .setTimestamp(resultSet.getLong("time_stamp"))
                                     .setAggregateType(type);

        if( type != null) {
            builder.setAggregateIdentifier(resultSet.getString("aggregate_identifier"));
        }
        return new SerializedEvent(builder.build());
    }

    @Override
    public Optional<SerializedEvent> getLastEvent(String aggregateIdentifier, long minSequenceNumber) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     readEventsForAggidDesc)) {
            preparedStatement.setString(1, aggregateIdentifier);
            preparedStatement.setLong(2, minSequenceNumber);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return Optional.of(readEvent(resultSet));
                }
            }

            return Optional.empty();
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber, long actualMaxSequenceNumber, int maxResults, Consumer<SerializedEvent> eventConsumer) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     readEventsForAggidWithinRangeDesc)) {
            preparedStatement.setString(1, aggregateId);
            preparedStatement.setLong(2, actualMinSequenceNumber);
            preparedStatement.setLong(3, actualMaxSequenceNumber);
            preparedStatement.setMaxRows(maxResults);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    eventConsumer.accept(readEvent(resultSet));
                }
            }
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber, Consumer<SerializedEvent> eventConsumer) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     readEventsForAggidAsc)) {
            preparedStatement.setString(1, aggregateId);
            preparedStatement.setLong(2, actualMinSequenceNumber);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    eventConsumer.accept(readEvent(resultSet));
                }
            }
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    protected String getTableName() {
        return multiContextStrategy.getTableName(eventTypeContext);
    }

    @Override
    public long getFirstToken() {
        long min = 0;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     minToken)) {
            min = getLongOrDefault(min, preparedStatement);
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
        return min;
    }

    @Override
    public long getTokenAt(long instant) {
        long min = -1;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     tokenAt)) {
            preparedStatement.setLong(1, instant);
            min = getLongOrDefault(min, preparedStatement);
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
        return min;
    }

    private long getLongOrDefault(long defaultValue, PreparedStatement preparedStatement) throws SQLException {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                Object value = resultSet.getObject(1);
                if (value != null) defaultValue = ((Number) value).longValue();
            }
        }
        return defaultValue;
    }

    @Override
    public void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer) {
        String query = String.format("select * from %s where global_index >= ? and time_stamp >= ? order by global_index desc", getTableName());
        try (Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(
                     query)) {
            preparedStatement.setLong(1, minToken);
            preparedStatement.setLong(2, minTimestamp);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while( resultSet.next()) {
                    SerializedEventWithToken serializedEventWithToken = readEventWithToken(resultSet);
                    if( ! consumer.test(serializedEventWithToken.asEventWithToken())) {
                        return;
                    }
                }

            }

        } catch (Exception e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }

    }
}
