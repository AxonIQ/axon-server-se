package io.axoniq.axonserver.migration.db;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface MigrationStatusRepository extends JpaRepository<MigrationStatus, Long> {
}
