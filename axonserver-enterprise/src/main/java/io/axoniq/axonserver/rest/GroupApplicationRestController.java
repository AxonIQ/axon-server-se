package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.access.application.ReplicationGroupApplication;
import io.axoniq.axonserver.access.application.ReplicationGroupApplicationRepository;
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
 * Created by marc on 7/14/2017.
 */
@RestController
@Api(tags = "internal")
@RequestMapping("/internal")
public class GroupApplicationRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final ReplicationGroupApplicationRepository applicationController;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public GroupApplicationRestController(ReplicationGroupApplicationRepository applicationController) {
        this.applicationController = applicationController;
    }

    @GetMapping("raft/applications")
    public ResponseEntity<String> getApplications(Principal principal) throws JsonProcessingException {
        auditLog.info("[{}] Request to list applications in replication groups.", AuditLog.username(principal));
        List<ReplicationGroupApplication> result = applicationController.findAll();
        return ResponseEntity.ok(objectMapper.writeValueAsString(result));
    }


}
