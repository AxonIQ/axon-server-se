package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.access.application.JpaContextApplication;
import io.axoniq.axonserver.access.application.JpaContextApplicationRepository;
import io.swagger.annotations.Api;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Created by marc on 7/14/2017.
 */
@RestController
@Api(tags = "internal")
@RequestMapping("/internal")
public class GroupApplicationRestController {

    private final JpaContextApplicationRepository applicationController;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public GroupApplicationRestController(JpaContextApplicationRepository applicationController) {
        this.applicationController = applicationController;
    }

    @GetMapping("raft/applications")
    public ResponseEntity<String> getApplications() throws JsonProcessingException {
        List<JpaContextApplication> result = applicationController.findAll();
        return ResponseEntity.ok(objectMapper.writeValueAsString(result));
    }


}
