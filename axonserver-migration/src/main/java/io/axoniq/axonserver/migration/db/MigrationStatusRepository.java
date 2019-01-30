package io.axoniq.axonserver.migration.db;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface MigrationStatusRepository extends JpaRepository<MigrationStatus, Long> {
}
