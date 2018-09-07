package io.axoniq.axonserver.context;

import io.axoniq.axonserver.context.jpa.Context;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface ContextRepository extends JpaRepository<Context, String> {
}
