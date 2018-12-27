package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.SafepointRepository;
import io.axoniq.axonserver.enterprise.jpa.Safepoint;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
                                  .map(Safepoint::safePoint)
                                  .orElse(-1L);
    }

}
