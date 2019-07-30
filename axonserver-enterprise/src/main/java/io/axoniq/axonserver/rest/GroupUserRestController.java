package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.access.application.JpaContextUser;
import io.axoniq.axonserver.access.application.JpaContextUserRepository;
import io.swagger.annotations.Api;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

    private final JpaContextUserRepository contextUserRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public GroupUserRestController(JpaContextUserRepository contextUserRepository) {
        this.contextUserRepository = contextUserRepository;
    }

    @GetMapping("raft/users")
    public ResponseEntity<String> getApplications() throws JsonProcessingException {
        List<JpaContextUser> result = contextUserRepository.findAll();
        return ResponseEntity.ok(objectMapper.writeValueAsString(result));
    }
}
