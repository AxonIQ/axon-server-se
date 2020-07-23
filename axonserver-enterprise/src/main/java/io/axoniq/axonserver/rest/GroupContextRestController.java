package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContextRepository;
import io.axoniq.axonserver.logging.AuditLog;
import io.swagger.annotations.Api;
import org.slf4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by marc on 7/14/2017.
 */
@RestController
@Api(tags = "internal")
@RequestMapping("/internal")
public class GroupContextRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final ReplicationGroupContextRepository replicationGroupContextRepository;

    public GroupContextRestController(ReplicationGroupContextRepository replicationGroupContextRepository) {
        this.replicationGroupContextRepository = replicationGroupContextRepository;
    }

    @GetMapping("raft/contexts")
    public ResponseEntity<List<ContextJSON>> getApplications(Principal principal) {
        auditLog.info("[{}] Request to list contexts in replication groups.", AuditLog.username(principal));
        List<ReplicationGroupContext> result = replicationGroupContextRepository.findAll();
        return ResponseEntity.ok(result.stream().map(r -> toContextJSON(r)).collect(Collectors.toList()));
    }

    private ContextJSON toContextJSON(ReplicationGroupContext r) {
        ContextJSON contextJSON = new ContextJSON();
        contextJSON.setContext(r.getName());
        contextJSON.setReplicationGroup(r.getReplicationGroupName());
        contextJSON.setMetaData(r.getMetaDataMap());
        return contextJSON;
    }
}
