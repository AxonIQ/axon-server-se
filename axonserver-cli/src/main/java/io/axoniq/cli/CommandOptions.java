/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.PatternOptionBuilder;

/**
 * @author Marc Gathier
 */
public class CommandOptions {
    public static final Option ADDRESS = Option.builder("S").hasArg().longOpt("server").desc("Server to send command to (default http://localhost:8024)")
                                               .build();
    public static final Option APPLICATION = Option.builder("a").hasArg().longOpt("application").required().desc("Name of the application").build();
    public static final Option ROLES = Option.builder("r").required().hasArgs().valueSeparator(',').longOpt("roles")
                                             .desc("Roles for the application, use role@context").build();
    public static final Option USER_ROLES = Option.builder("r").hasArgs().valueSeparator(',').longOpt("roles").desc(
            "[Optional] roles for the user").build();
    public static final Option DESCRIPTION = Option.builder("d").hasArgs().longOpt("description").desc("[Optional] Description of the application").build();
    public static final Option NODENAME = Option.builder("n").longOpt("node").required().hasArg().desc("Name of the node").build();
    public static final Option NODEROLE = Option.builder("r").longOpt("role").hasArg().desc(
            "Role of the node (PRIMARY,MESSAGING_ONLY,ACTIVE_BACKUP,PASSIVE_BACKUP").build();
    public static final Option CONTEXT = Option.builder("c").longOpt("context").required().hasArg().desc("Name of the context").build();
    public static final Option NODES = Option.builder("n").hasArgs().valueSeparator(',').longOpt("nodes").desc("[Optional - Enterprise Edition only] member nodes for context").build();
    public static final Option CONTEXT_TO_REGISTER_IN = Option.builder("c").longOpt("context").hasArg().desc("[Optional - Enterprise Edition only] context to register node in").build();
    public static final Option DONT_REGISTER_IN_CONTEXTS = Option.builder().longOpt("no-contexts").desc("[Optional - Enterprise Edition only] add node to cluster, but don't register it in any contexts").build();
    public static final Option PRESERVE_EVENT_STORE = Option.builder().longOpt("preserve-event-store").desc(
            "[Optional - Enterprise Edition only] keep event store contents").build();
    public static final Option INTERNALHOST = Option.builder("h").longOpt("internal-host").desc("Internal hostname of the node").required().hasArg().build();
    public static final Option INTERNALPORT = Option.builder("p").longOpt("internal-port").desc("Internal port of the node (default 8224)").type(PatternOptionBuilder.NUMBER_VALUE).hasArg().build();
    public static final Option TOKEN = Option.builder("t").longOpt("access-token").desc("[Optional] Access token to authenticate at server").hasArg().build();
    public static final Option USERNAME = Option.builder("u").longOpt("username").required().desc("Username").hasArg().build();
    public static final Option PASSWORD = Option.builder("p").longOpt("password").desc("[Optional] Password for the user").hasArg().build();
    public static final Option SET_TOKEN = Option.builder("T").longOpt("token").desc("use this token for the app").hasArg().build();
    public static final Option OUTPUT = Option.builder("o").hasArg().longOpt("output").desc("Output format (txt,json)").build();
}
