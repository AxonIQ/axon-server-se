package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.enterprise.jpa.Context;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface ContextRepository extends JpaRepository<Context, String> {
}
