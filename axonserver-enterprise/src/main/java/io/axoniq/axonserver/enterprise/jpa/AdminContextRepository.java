package io.axoniq.axonserver.enterprise.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface AdminContextRepository extends JpaRepository<AdminContext, String> {

}
