package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.access.application.SystemTokenProvider;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Returns the generated access token. Available only when server is started with profile "testing-only". Created to support
 * test automation. Do not enable in production systems.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@RestController
@CrossOrigin
@Profile("testing-only")
@RequestMapping("internal/systemtoken")
public class SystemTokenRestController {

    private final SystemTokenProvider systemTokenProvider;

    public SystemTokenRestController(SystemTokenProvider systemTokenProvider) {
        this.systemTokenProvider = systemTokenProvider;
    }

    /**
     * Returns the system generated token.
     *
     * @return the generated system token
     */
    @GetMapping
    public String systemToken() {
        return systemTokenProvider.get();
    }
}
