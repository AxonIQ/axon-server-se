package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.SafepointRepository;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.jpa.Safepoint;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;
import javax.validation.Valid;

/**
 * Author: marc
 */
@RestController
@CrossOrigin
@RequestMapping("/v1")
public class SafepointRestController {

    private final SafepointRepository safepointRepository;

    public SafepointRestController(SafepointRepository safepointRepository) {
        this.safepointRepository = safepointRepository;
    }

    @GetMapping(path = "safepoints")
    public long safepoint(@RequestParam(value = "type", defaultValue = "EVENT", required = false) String type,
                          @RequestParam(value = "context", defaultValue = "default", required = false) String context) {
        return safepointRepository.findById(new Safepoint.SafepointKey(context, type))
                                  .map(Safepoint::getToken)
                                  .orElse(-1L);
    }

}
