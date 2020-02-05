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
 * Constants for command line options for the various CLI commands.
 * @author Marc Gathier
 * @since 4.0
 */
public class CommandOptions {

    /**
     * The base URL for Axon Server admin node.
     */
    public static final Option ADDRESS = Option.builder("S").hasArg().longOpt("server").desc("Server to send command to (default http://localhost:8024)")
                                               .build();
    /**
     * The name of the application to manage.
     */
    public static final Option APPLICATION = Option.builder("a").hasArg().longOpt("application").required().desc("Name of the application").build();
    /**
     * The roles granted to the application.
     */
    public static final Option APPLICATION_ROLES = Option.builder("r").required().hasArgs().valueSeparator(',').longOpt(
            "roles")
                                                         .desc("Roles for the application, use role@context").build();
    /**
     * The roles granted to the user.
     */
    public static final Option USER_ROLES = Option.builder("r").hasArgs().valueSeparator(',').longOpt("roles").desc(
            "[Optional] roles for the user").build();
    /**
     * The description of the application.
     */
    public static final Option APPLICATION_DESCRIPTION = Option.builder("d").hasArgs().longOpt("description").desc(
            "[Optional] Description of the application").build();
    /**
     * The name of the node to add/remove from the context.
     */
    public static final Option NODE_NAME = Option.builder("n").longOpt("node").required().hasArg().desc(
            "Name of the node").build();
    /**
     * The role of the node in the context.
     */
    public static final Option NODE_ROLE = Option.builder("r").longOpt("role").hasArg().desc(
            "Role of the node (PRIMARY,MESSAGING_ONLY,ACTIVE_BACKUP,PASSIVE_BACKUP").build();
    /**
     * The name of the context.
     */
    public static final Option CONTEXT = Option.builder("c").longOpt("context").required().hasArg().desc(
            "Name of the context").build();
    /**
     * Comma separated list of Axon Server node names as primary members for the context.
     */
    public static final Option NODES = Option.builder("n")
                                             .hasArgs()
                                             .valueSeparator(',')
                                             .longOpt("nodes")
                                             .required()
                                             .desc("[Enterprise Edition only] primary member nodes for context")
                                             .build();
    /**
     * Comma separated list of Axon Server node names as active backup nodes for the context.
     */
    public static final Option ACTIVE_BACKUP_NODES = Option.builder("a").hasArgs().valueSeparator(',').longOpt(
            "active-backup").desc("[Optional - Enterprise Edition only] active backup member nodes for context")
                                                           .build();
    /**
     * Comma separated list of Axon Server node names as passive backup nodes for the context.
     */
    public static final Option PASSIVE_BACKUP_NODES = Option.builder("p").hasArgs().valueSeparator(',').longOpt(
            "passive-backup").desc("[Optional - Enterprise Edition only] passive backup member nodes for context")
                                                            .build();
    /**
     * Comma separated list of Axon Server node names as messaging-only nodes for the context.
     */
    public static final Option MESSAGING_ONLY_NODES = Option.builder("m")
                                                            .hasArgs()
                                                            .valueSeparator(',')
                                                            .longOpt("messaging-only").desc(
                    "[Optional - Enterprise Edition only] messaging-only member nodes for context").build();
    /**
     * The name of the context, where the nodes should be added to.
     */
    public static final Option CONTEXT_TO_REGISTER_IN = Option.builder("c").longOpt("context").hasArg().desc(
            "[Optional - Enterprise Edition only] context to register node in").build();
    /**
     * Indicator to register a node without adding it to any contexts.
     */
    public static final Option DONT_REGISTER_IN_CONTEXTS = Option.builder().longOpt("no-contexts").desc(
            "[Optional - Enterprise Edition only] add node to cluster, but don't register it in any contexts").build();
    /**
     * While removing a node from a context, preserve the event store to be able to add it again (with a different
     * role), without the need to
     * copy all events again.
     */
    public static final Option PRESERVE_EVENT_STORE = Option.builder().longOpt("preserve-event-store").desc(
            "[Optional - Enterprise Edition only] keep event store contents").build();
    /**
     * The internal hostname of the AxonServer node to register the node to.
     */
    public static final Option INTERNALHOST = Option.builder("h").longOpt("internal-host").desc("Internal hostname of the node").required().hasArg().build();
    /**
     * The internal gRPC port number of the Axon Server to to register the node to.
     */
    public static final Option INTERNALPORT = Option.builder("p").longOpt("internal-port").desc("Internal port of the node (default 8224)").type(PatternOptionBuilder.NUMBER_VALUE).hasArg().build();
    /**
     * Token to use when sending CLI commands.
     */
    public static final Option TOKEN = Option.builder("t").longOpt("access-token").desc("[Optional] Access token to authenticate at server").hasArg().build();
    /**
     * The username for the user to manage.
     */
    public static final Option USERNAME = Option.builder("u").longOpt("username").required().desc("Username").hasArg().build();
    /**
     * The password for the user to create.
     */
    public static final Option PASSWORD = Option.builder("p").longOpt("password").desc("[Optional] Password for the user").hasArg().build();
    /**
     * Defines the token for a new application. If this is omitted Axon Server will generate a token.
     */
    public static final Option SET_TOKEN = Option.builder("T").longOpt("token").desc("use this token for the app").hasArg().build();
    /**
     * Output format for the command (text or json).
     */
    public static final Option OUTPUT = Option.builder("o").hasArg().longOpt("output").desc("Output format (txt,json)").build();
}
