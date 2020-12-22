/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.taskscheduler.BaseTaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Retrieves important info about Axon Server
 * such as third-party licences
 *
 * @author Stefan Dragisic
 * @since 4.5
 */
@RestController("InfoRestController")
@RequestMapping("/v1/info")
public class InfoRestController {

    protected static final Logger logger = LoggerFactory.getLogger(BaseTaskManager.class);

    @GetMapping("/third-party")
    public  String thirdParty() {
        try {
            List<String> strings = Files.readAllLines(
                    Paths.get(this.getClass().getResource("/third-party-licenses.txt").toURI()), Charset.defaultCharset());

            return String.join("", strings);
        } catch ( Exception e) {
            logger.warn("Third-party licences could not be retrieved... Error: ",e);
        }
        return "";

    }

}
