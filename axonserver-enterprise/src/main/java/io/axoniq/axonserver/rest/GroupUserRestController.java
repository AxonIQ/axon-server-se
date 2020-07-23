package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.access.user.ReplicationGroupUser;
import io.axoniq.axonserver.access.user.ReplicationGroupUserRepository;
import io.axoniq.axonserver.logging.AuditLog;
import io.swagger.annotations.Api;
import org.slf4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.List;

/**
 * Internal REST API to retrieve the users defined in the CONTEXT_USERS table.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@RestController
@Api(tags = "internal")
@RequestMapping("/internal")
public class GroupUserRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final ReplicationGroupUserRepository contextUserRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public GroupUserRestController(ReplicationGroupUserRepository contextUserRepository) {
        this.contextUserRepository = contextUserRepository;
    }

    @GetMapping("raft/users")
    public ResponseEntity<String> getApplications(Principal principal) throws JsonProcessingException {
        auditLog.info("[{}] Request to list contexts in replication groups.", AuditLog.username(principal));
        List<ReplicationGroupUser> result = contextUserRepository.findAll();
        return ResponseEntity.ok(objectMapper.writeValueAsString(result));
    }
}
