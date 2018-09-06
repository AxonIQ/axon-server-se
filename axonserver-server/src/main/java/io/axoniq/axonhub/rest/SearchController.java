package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.context.ContextController;
import io.axoniq.axonhub.exception.ErrorCode;
import io.axoniq.axonhub.exception.MessagingPlatformException;
import io.axoniq.axonhub.licensing.Limits;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Author: marc
 */
@RestController
@CrossOrigin
@RequestMapping("/v1/search")

public class SearchController {
    private final HttpStreamingQuery httpStreamingQuery;
    private final Limits limits;

    @Value("${axoniq.axondb.query.timeout:300000}")
    private long timeout = 300000;

    public SearchController(HttpStreamingQuery httpStreamingQuery, Limits limits) {
        this.httpStreamingQuery = httpStreamingQuery;
        this.limits = limits;
    }


    @GetMapping
    public SseEmitter query(@RequestParam(value = "context", defaultValue = ContextController.DEFAULT) String context,
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
