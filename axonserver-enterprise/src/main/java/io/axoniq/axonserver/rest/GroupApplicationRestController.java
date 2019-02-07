package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.access.application.JpaRaftGroupApplicationRepository;
import io.axoniq.axonserver.enterprise.jpa.JpaRaftGroupApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Created by marc on 7/14/2017.
 */
@RestController
@RequestMapping("/v1")
public class GroupApplicationRestController {

    private final JpaRaftGroupApplicationRepository applicationController;

    public GroupApplicationRestController(JpaRaftGroupApplicationRepository applicationController) {
        this.applicationController = applicationController;
    }

    @GetMapping("raft/applications")
    public List<JpaRaftGroupApplication> getApplications() {
        return applicationController.findAll();
    }


}
