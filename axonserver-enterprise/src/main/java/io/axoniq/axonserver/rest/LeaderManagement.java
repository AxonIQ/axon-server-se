package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.LocalRaftGroupService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: marc
 */
@RestController
@CrossOrigin
@RequestMapping("/v1")
public class LeaderManagement {
    private final LocalRaftGroupService localRaftGroupService;

    public LeaderManagement(LocalRaftGroupService localRaftGroupService) {
        this.localRaftGroupService = localRaftGroupService;
    }

    @GetMapping( path = "context/{name}/stepdown")
    public void stepdown(@PathVariable("name")  String name) {
        localRaftGroupService.stepDown(name);
    }

}
