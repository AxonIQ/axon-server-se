package io.axoniq.axonhub.context;

import io.axoniq.axonhub.context.jpa.Context;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface ContextRepository extends JpaRepository<Context, String> {
}
