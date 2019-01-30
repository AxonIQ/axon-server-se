package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * @author Marc Gathier
 */
@RestController
@CrossOrigin
@RequestMapping("/v1/search")

public class SearchController {
    private final HttpStreamingQuery httpStreamingQuery;
    private final FeatureChecker limits;

    @Value("${axoniq.axonserver.query.timeout:300000}")
    private long timeout = 300000;

    public SearchController(HttpStreamingQuery httpStreamingQuery, FeatureChecker limits) {
        this.httpStreamingQuery = httpStreamingQuery;
        this.limits = limits;
    }


    @GetMapping
    public SseEmitter query(@RequestParam(value = "context", defaultValue = Topology.DEFAULT_CONTEXT) String context,
                            @RequestParam("query") String query,
                            @RequestParam("clientToken") String clientToken) {
        SseEmitter sseEmitter = new SseEmitter(timeout);
        if( ! Feature.AD_HOC_QUERIES.enabled(limits)) {
            sseEmitter.completeWithError(new MessagingPlatformException(ErrorCode.AUTHENTICATION_INVALID_TOKEN, "Ad-hoc query not allowed"));
        } else {
            httpStreamingQuery.query(context, query, clientToken, sseEmitter);
        }
        return sseEmitter;
    }


}
