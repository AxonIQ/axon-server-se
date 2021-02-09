/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.topology.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import springfox.documentation.annotations.ApiIgnore;

import static io.axoniq.axonserver.localstorage.query.QueryEventsRequestStreamObserver.TIME_WINDOW_CUSTOM;

/**
 * Rest service to perform queries on the event store.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@RestController
@CrossOrigin
@RequestMapping("/v1/search")
public class SearchController {
    private final HttpStreamingQuery httpStreamingQuery;

    @Value("${axoniq.axonserver.query.timeout:300000}")
    private long timeout = 300000;

    public SearchController(HttpStreamingQuery httpStreamingQuery) {
        this.httpStreamingQuery = httpStreamingQuery;
    }


    @GetMapping
    public SseEmitter query(@RequestParam(value = "context", defaultValue = Topology.DEFAULT_CONTEXT) String context,
                            @RequestParam("query") String query,
                            @RequestParam(value = "timewindow", required = false, defaultValue = TIME_WINDOW_CUSTOM) String timewindow,
                            @RequestParam(value = "liveupdates", required = false, defaultValue = "false") Boolean liveupdates,
                            @RequestParam(value = "forceleader", required = false, defaultValue = "false") Boolean forceReadFromLeader,
                            @RequestParam(value = "querySnapshots", required = false, defaultValue = "false") Boolean querySnapshots,
                            @RequestParam("clientToken") String clientToken,
                            @ApiIgnore Authentication authentication) {
        SseEmitter sseEmitter = new SseEmitter(timeout);
        httpStreamingQuery.query(context,
                                 authentication,
                                 query,
                                 timewindow,
                                 liveupdates,
                                 forceReadFromLeader,
                                 clientToken,
                                 sseEmitter, querySnapshots);
        return sseEmitter;
    }


}
