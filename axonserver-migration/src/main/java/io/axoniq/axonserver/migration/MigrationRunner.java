package io.axoniq.axonserver.migration;


import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.migration.db.MigrationStatus;
import io.axoniq.axonserver.migration.db.MigrationStatusRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.PlatformConnectionManager;
import org.axonframework.axonserver.connector.event.AppendEventTransaction;
import org.axonframework.axonserver.connector.event.AxonDBClient;
import org.axonframework.axonserver.connector.util.GrpcMetaDataConverter;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Author: marc
 */
@Component
public class MigrationRunner implements CommandLineRunner {
    private final EventProducer eventProducer;
    private final AxonDBClient axonDBClient;
    private final Serializer serializer;
    private final MigrationStatusRepository migrationStatusRepository;
    private final Logger logger = LoggerFactory.getLogger(MigrationRunner.class);
    private final Counter eventsMigrated;
    private final Counter snapshotsMigrated;
    private final MetricReporter metricReporter;
    private final GrpcMetaDataConverter grpcMetaDataConverter;

    @Value("${axoniq.migration.batchSize:100}")
    private int batchSize;
    @Value("${axoniq.migration.recentMillis:10000}")
    private int recentMillis;

    public MigrationRunner(EventProducer eventProducer, Serializer serializer,
                           MigrationStatusRepository migrationStatusRepository, MeterRegistry meterRegistry,
                           MetricReporter metricReporter, AxonDBClient axonDBClient
                           ) {
        this.eventProducer = eventProducer;
        this.axonDBClient = axonDBClient;
        this.serializer = serializer;
        this.migrationStatusRepository = migrationStatusRepository;
        this.metricReporter = metricReporter;
        this.grpcMetaDataConverter = new GrpcMetaDataConverter(this.serializer);
        this.eventsMigrated = meterRegistry.counter(name(this.getClass(), "eventsMigrated"));
        this.snapshotsMigrated = meterRegistry.counter(name(this.getClass(), "snapshotsMigrated"));
    }

    @Override
    public void run(String... options) throws Exception {
        metricReporter.beginReporting();
        try {
            migrateEvents();
            migrateSnapshots();
        } finally {
            metricReporter.endReporting();
        }
    }

    private void migrateSnapshots() {
        MigrationStatus migrationStatus = migrationStatusRepository.findById(1L).orElse(new MigrationStatus());

        String lastProcessedTimestamp = migrationStatus.getLastSnapshotTimestamp();
        String lastEventId = migrationStatus.getLastSnapshotEventId();

        boolean keepRunning = true;

        logger.info("Starting migration of snapshots from timestamp: {}, batchSize = {}", lastProcessedTimestamp, batchSize);

        try {
            while(keepRunning) {
                keepRunning = false;
                List<? extends SnapshotEvent> result = eventProducer.findSnapshots(lastProcessedTimestamp, batchSize);
                if( result.size() == 0) {
                    logger.info("No more snapshots found");
                    return;
                }
                boolean lastFound = (lastEventId == null);
                for (SnapshotEvent entry : result) {
                    if (!lastFound ) {
                        lastFound = entry.getTimeStamp().compareTo(lastProcessedTimestamp) > 0 || entry.getEventIdentifier().equals(lastEventId);
                        continue;
                    }

                    Event.Builder eventBuilder = Event.newBuilder().setAggregateIdentifier(entry.getAggregateIdentifier())
                                                      .setPayload(toPayload(entry))
                                                      .setAggregateSequenceNumber(entry.getSequenceNumber())
                                                      .setMessageIdentifier(entry.getEventIdentifier())
                                                      .setAggregateType(entry.getType());

                    eventBuilder.setTimestamp(entry.getTimeStampAsLong());
                    convertMetadata(entry.getMetaData(), eventBuilder);

                    axonDBClient.appendSnapshot(eventBuilder.build());
                    lastProcessedTimestamp = entry.getTimeStamp();
                    lastEventId = entry.getEventIdentifier();
                    snapshotsMigrated.increment();
                    keepRunning = true;
                }
            }
        } finally {
            migrationStatus.setLastSnapshotEventId(lastEventId);
            migrationStatus.setLastSnapshotTimestamp(lastProcessedTimestamp);
            migrationStatusRepository.save(migrationStatus);
        }


    }

    private void convertMetadata(byte[] metadataBytes, Event.Builder eventBuilder) {
        if (metadataBytes != null) {
            MetaData metaData = serializer.deserialize(new SerializedMetaData<>(metadataBytes, byte[].class));
            Map<String, MetaDataValue> metaDataValues = new HashMap<>();
            metaData.forEach((k, v) -> {
                metaDataValues.put(k, grpcMetaDataConverter.convertToMetaDataValue(v));
            });
            eventBuilder.putAllMetaData(metaDataValues);
        }
    }

    private void migrateEvents() throws InterruptedException, ExecutionException, TimeoutException {
        MigrationStatus migrationStatus = migrationStatusRepository.findById(1L).orElse(new MigrationStatus());

        long lastProcessedToken = migrationStatus.getLastEventGlobalIndex();
        boolean keepRunning = true;

        logger.info("Starting migration of event from globalIndex: {}, batchSize = {}", lastProcessedToken, batchSize);

        while(keepRunning) {
            List<? extends DomainEvent> result = eventProducer.findEvents(lastProcessedToken, batchSize);
            if( result.size() == 0) {
                logger.info("No more events found");
                return;
            }
            AppendEventTransaction appendEventConnection = axonDBClient.createAppendEventConnection();
            for( DomainEvent entry : result) {

                if( entry.getGlobalIndex() != lastProcessedToken + 1 && recentEvent(entry)) {
                    logger.error("Missing event at: {}, found globalIndex {}, stopping migration", (lastProcessedToken + 1), entry.getGlobalIndex());
                    keepRunning = false;
                    break;
                }

                Event.Builder eventBuilder = Event.newBuilder()
                        .setPayload(toPayload(entry))
                        .setMessageIdentifier(entry.getEventIdentifier())
                        ;

                if( entry.getType()!= null) {
                    eventBuilder.setAggregateType(entry.getType())
                                .setAggregateSequenceNumber(entry.getSequenceNumber())
                                .setAggregateIdentifier(entry.getAggregateIdentifier())
                    ;
                }

                eventBuilder.setTimestamp(entry.getTimeStampAsLong());
                convertMetadata(entry.getMetaData(), eventBuilder);

                appendEventConnection.append(eventBuilder.build());
                lastProcessedToken = entry.getGlobalIndex();
                eventsMigrated.increment();
            }
            appendEventConnection.commit();
            migrationStatus.setLastEventGlobalIndex(lastProcessedToken);
            migrationStatusRepository.save(migrationStatus);
        }


    }

    private boolean recentEvent(DomainEvent entry) {
        return entry.getTimeStampAsLong() > System.currentTimeMillis() - recentMillis;
    }

    private SerializedObject toPayload(BaseEvent entry) {
        SerializedObject.Builder builder = SerializedObject.newBuilder()
                                                           .setData(ByteString.copyFrom(entry.getPayload()))
                                                           .setType(entry.getPayloadType());

        if( entry.getPayloadRevision() != null)
            builder.setRevision(entry.getPayloadRevision());
        return builder.build();
    }
}
