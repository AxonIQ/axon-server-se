package io.axoniq.axonserver.refactoring.store.api;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 4.6
 */
public interface SnapshotStorage {

    Mono<Snapshot> latestSnapshot(String aggregateId);

    Mono<Void> appendSnapshot(Snapshot snapshot);

    Flux<Snapshot> aggregateSnapshots(AggregateSnapshotsQuery query);

    Flux<SnapshotQueryResponse> querySnapshots(AdHocSnapshotsQuery query);

}
