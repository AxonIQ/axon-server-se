package io.axoniq.axonserver.enterprise.storage.jdbc;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;
import io.axoniq.axonserver.localstorage.transformation.WrappedEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.sql.DataSource;

/**
 * Author: marc
 */
public abstract class JdbcAbstractStore implements EventStore {

    private final String maxGlobalIndex = String.format("select max(global_index) from %s", getTableName());
    private final String createTable = String.format("create table %s (global_index bigint not null, aggregate_identifier varchar(255) not null, event_identifier varchar(255) not null, meta_data blob, payload blob not null, payload_revision varchar(255), payload_type varchar(255) not null, sequence_number bigint not null, time_stamp bigint not null, type varchar(255) not null, primary key (global_index))", getTableName());
    private final String createIndexAggidSeqnr = String.format("alter table %s add constraint %s_uk1 unique (aggregate_identifier, sequence_number)", getTableName(), getTableName());
    private final String createIndexEventId = String.format("alter table %s add constraint %s_uk2 unique (event_identifier)", getTableName(), getTableName());
    private final String insertEvent = String.format("insert into %s(global_index, aggregate_identifier, event_identifier, meta_data, payload, payload_revision, payload_type, sequence_number, time_stamp, type) values (?,?,?,?,?,?,?,?,?,?)", getTableName());
    private final String maxSeqnrForAggid = String.format("select max(sequence_number) from %s where aggregate_identifier = ?", getTableName());
    private final String readEvents = String.format("select * from %s where global_index >= ? order by global_index asc", getTableName());

    private final String readEventsForAggidWithinRangeDesc = String.format("select * from %s where aggregate_identifier = ? and sequence_number >= ? and sequence_number <= ? order by sequence_number desc", getTableName());
    private final String readEventsForAggidDesc = String.format("select * from %s where aggregate_identifier = ? and sequence_number >= ? order by sequence_number desc", getTableName());
    private final String readEventsForAggidAsc = String.format("select * from %s where aggregate_identifier = ? and sequence_number >= ? order by sequence_number asc", getTableName());
    private final String tokenAt = String.format("select min(token) from %s where time_stamp >= ?", getTableName());
    private final String minToken = String.format("select min(token) from %s", getTableName());

    private final AtomicLong lastToken = new AtomicLong(-1);
    private final EventTypeContext eventTypeContext;
    private final DataSource dataSource;

    protected JdbcAbstractStore(EventTypeContext eventTypeContext,
                             DataSource dataSource) {
        this.eventTypeContext = eventTypeContext;
        this.dataSource = dataSource;
    }

    @Override
    public EventTypeContext getType() {
        return eventTypeContext;
    }

    @Override
    public Iterator<TransactionWithToken> transactionIterator(long firstToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<TransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void streamTransactions(long firstToken, Predicate<TransactionWithToken> transactionConsumer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void init(boolean validating) {
        try (Connection connection = dataSource.getConnection()) {
            boolean tableExists;
            try (ResultSet resultSet = connection.getMetaData().getTables(null, null, getTableName(), null)) {
                tableExists = resultSet.next();
            }
            if (!tableExists) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(
                            createTable);
                }
                try (Statement statement = connection.createStatement()) {
                    statement.execute(
                            createIndexAggidSeqnr);
                }
                try (Statement statement = connection.createStatement()) {
                    statement.execute(
                            createIndexEventId);
                }
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(
                    maxGlobalIndex);
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    Number last = (Number)resultSet.getObject(1);
                    if( last != null)lastToken.set(last.longValue());
                }
            }
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        }
    }

    @Override
    public PreparedTransaction prepareTransaction(List<Event> eventList) {
        long firstToken = lastToken.getAndAdd(eventList.size()) + 1;
        return new PreparedTransaction(firstToken, eventList.stream().map(WrappedEvent::new).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Long> store(PreparedTransaction preparedTransaction) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            long firstToken = preparedTransaction.getToken();
            try (PreparedStatement insert = connection.prepareStatement(
                    insertEvent)) {
                for (ProcessedEvent event : preparedTransaction.getEventList()) {
                    insert.setLong(1, firstToken++);
                    insert.setString(2, event.getAggregateIdentifier());
                    insert.setString(3, event.getMessageIdentifier());
                    insert.setNull(4, Types.BLOB);
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
        return lastToken.get();
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     maxSeqnrForAggid)) {
            preparedStatement.setString(1, aggregateIdentifier);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    Number last = (Number)resultSet.getObject(1);
                    if( last != null) return Optional.of(last.longValue());
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public boolean streamEvents(long token,
                             Predicate<EventWithToken> onEvent) {
        try (Connection connection = dataSource.getConnection();
                     PreparedStatement preparedStatement = connection.prepareStatement(
                             readEvents)) {
            EventWithToken event;
            preparedStatement.setLong(1, token);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    event = readEventWithToken(resultSet);
                    if (!onEvent.test(event)) {
                        return false;
                    }
                }
            }
            return true;
        } catch (SQLException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    private EventWithToken readEventWithToken(ResultSet resultSet) throws SQLException {
        return EventWithToken.newBuilder()
                             .setToken(resultSet.getLong("global_index"))
                             .setEvent(readEvent(resultSet))
                             .build();
    }

    private Event readEvent(ResultSet resultSet) throws SQLException {
        return Event.newBuilder()
                    .setAggregateIdentifier(resultSet.getString("aggregate_identifier"))
                    .setAggregateSequenceNumber(resultSet.getLong("sequence_number"))
                    .setMessageIdentifier(resultSet.getString("event_identifier"))
                    .setPayload(SerializedObject.newBuilder()
                                                .setData(ByteString.copyFrom(resultSet.getBytes("payload")))
                                                .setRevision(resultSet.getString("payload_revision"))
                                                .setType(resultSet.getString("payload_type")))
                    .setTimestamp(resultSet.getLong("time_stamp"))
                    .setAggregateType(resultSet.getString("type")).build();
    }

    @Override
    public Optional<Event> getLastEvent(String aggregateIdentifier, long minSequenceNumber) {
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
    public void streamByAggregateId(String aggregateId, long actualMinSequenceNumber, long actualMaxSequenceNumber, int maxResults, Consumer<Event> eventConsumer) {
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
    public void streamByAggregateId(String aggregateId, long actualMinSequenceNumber, Consumer<Event> eventConsumer) {
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

    protected abstract String getTableName();

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

    }
}
