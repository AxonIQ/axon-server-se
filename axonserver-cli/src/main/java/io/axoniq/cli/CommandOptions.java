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
    public static final Option ADDRESS = Option
            .builder("S")
            .longOpt("server")
            .hasArg()
            .desc("Server to send command to (default http://localhost:8024)")
            .build();

    /**
     * Connect to Axon Server using HTTPS instead of HTTP.
     */
    public static final Option USE_HTTPS = Option
            .builder("s")
            .longOpt("https")
            .desc("Use HTTPS to connect to the server, rather than HTTP.")
            .build();

    /**
     * Connect to Axon Server using HTTPS instead of HTTP.
     */
    public static final Option CONNECT_INSECURE = Option
            .builder("i")
            .longOpt("insecure-ssl")
            .desc("Do not check the certificate when connecting using HTTPS.")
            .build();

    /**
     * The name of the application to manage.
     */
    public static final Option APPLICATION = Option
            .builder("a")
            .longOpt("application")
            .hasArg()
            .required()
            .desc("Name of the application")
            .build();
    /**
     * The name of the extension to manage.
     */
    public static final Option EXTENSION_NAME = Option
            .builder("e")
            .longOpt("extension")
            .hasArg()
            .required()
            .desc("Name of the extension")
            .build();
    /**
     * The version of the extension to manage.
     */
    public static final Option EXTENSION_VERSION = Option
            .builder("v")
            .longOpt("version")
            .hasArg()
            .required()
            .desc("Version of the extension")
            .build();
    /**
     * The context for the extension
     */
    public static final Option EXTENSION_CONTEXT = Option
            .builder("c")
            .longOpt("context")
            .hasArg()
            .desc("Context")
            .build();
    /**
     * The roles granted to the application.
     */
    public static final Option APPLICATION_ROLES = Option
            .builder("r")
            .longOpt("roles")
            .hasArgs().valueSeparator(',')
            .required()
            .desc("Roles for the application, use role@context")
            .build();
    /**
     * The roles granted to the user.
     */
    public static final Option USER_ROLES = Option
            .builder("r")
            .longOpt("roles")
            .hasArgs().valueSeparator(',')
            .desc("[Optional] roles for the user")
            .build();
    /**
     * The description of the application.
     */
    public static final Option APPLICATION_DESCRIPTION = Option
            .builder("d")
            .longOpt("description")
            .hasArgs() // space separator
            .desc("[Optional] Description of the application")
            .build();
    /**
     * The name of the node to add/remove from the context.
     */
    public static final Option NODE_NAME = Option
            .builder("n")
            .longOpt("node")
            .hasArg()
            .required()
            .desc("Name of the node")
            .build();
    /**
     * The role of the node in the context.
     */
    public static final Option NODE_ROLE = Option
            .builder("r")
            .longOpt("role")
            .hasArg()
            .desc("Role of the node (PRIMARY,MESSAGING_ONLY,ACTIVE_BACKUP,PASSIVE_BACKUP,SECONDARY")
            .build();
    /**
     * The name of the context.
     */
    public static final Option CONTEXT = Option
            .builder("c")
            .longOpt("context")
            .required()
            .hasArg()
            .desc("Name of the context")
            .build();
    /**
     * The name of the replication group.
     */
    public static final Option REPLICATIONGROUP = Option
            .builder("g")
            .longOpt("replication-group")
            .required()
            .hasArg()
            .desc("Name of the replication group")
            .build();

    public static final Option CONTEXTREPLICATIONGROUP = Option
            .builder("g")
            .longOpt("replication-group")
            .hasArg()
            .desc("Name of the replication group")
            .build();
    /**
     * Comma separated list of Axon Server node names as primary members for the replication group.
     */
    public static final Option PRIMARY_NODES = Option
            .builder("n")
            .longOpt("nodes")
            .hasArgs().valueSeparator(',')
            .required()
            .desc("Primary member nodes for replication group")
            .build();
    /**
     * Comma separated list of Axon Server node names as primary members for the replication group as created during
     * create context.
     */
    public static final Option CONTEXT_PRIMARY_NODES = Option
            .builder("n")
            .longOpt("nodes")
            .hasArgs().valueSeparator(',')
            .desc("Primary member nodes for replication group when creating as part of context")
            .build();
    /**
     * Comma separated list of Axon Server node names as active backup nodes for the replication group.
     */
    public static final Option ACTIVE_BACKUP_NODES = Option
            .builder("a")
            .longOpt("active-backup")
            .hasArgs().valueSeparator(',')
            .desc("[Optional] active backup member nodes for replication group")
            .build();
    /**
     * Comma separated list of Axon Server node names as passive backup nodes for the replication group.
     */
    public static final Option PASSIVE_BACKUP_NODES = Option
            .builder("p")
            .longOpt("passive-backup")
            .hasArgs().valueSeparator(',')
            .desc("[Optional] passive backup member nodes for replication group")
            .build();
    /**
     * Comma separated list of Axon Server node names as messaging-only nodes for the replication group.
     */
    public static final Option MESSAGING_ONLY_NODES = Option
            .builder("m")
            .hasArgs()
            .valueSeparator(',')
            .longOpt("messaging-only")
            .desc("[Optional] messaging-only member nodes for replication group")
            .build();
    /**
     * Comma separated list of Axon Server node names as secondary nodes for the replication group.
     */
    public static final Option SECONDARY_NODES = Option
            .builder("s")
            .hasArgs()
            .valueSeparator(',')
            .longOpt("secondary")
            .desc("[Optional] secondary member nodes for replication group")
            .build();
    /**
     * Properties that can be set on a context or application. Values are in the form name=value
     */
    public static final Option PROPERTIES = Option
            .builder("prop")
            .hasArgs()
            .valueSeparator(',')
            .longOpt("property")
            .desc("[Optional] properties for a context/application (specify as name=value)")
            .build();
    /**
     * The name of the context, where the nodes should be added to.
     */
    public static final Option CONTEXT_TO_REGISTER_IN = Option
            .builder("c")
            .longOpt("context")
            .hasArg()
            .desc("[Optional] context to register node in")
            .build();
    /**
     * Indicator to register a node without adding it to any contexts.
     */
    public static final Option DONT_REGISTER_IN_CONTEXTS = Option
            .builder()
            .longOpt("no-contexts")
            .desc("[Optional] add node to cluster, but don't register it in any contexts")
            .build();
    /**
     * While removing a node from a context, preserve the event store to be able to add it again (with a different
     * role), without the need to
     * copy all events again.
     */
    public static final Option PRESERVE_EVENT_STORE = Option
            .builder()
            .longOpt("preserve-event-store")
            .desc("[Optional] keep event store contents")
            .build();
    /**
     * The internal hostname of the AxonServer node to register the node to.
     */
    public static final Option INTERNALHOST = Option
            .builder("h")
            .longOpt("internal-host")
            .hasArg()
            .required()
            .desc("Internal hostname of the node")
            .build();
    /**
     * The internal gRPC port number of the Axon Server to to register the node to.
     */
    public static final Option INTERNALPORT = Option
            .builder("p")
            .longOpt("internal-port")
            .hasArg()
            .type(PatternOptionBuilder.NUMBER_VALUE)
            .desc("Internal port of the node (default 8224)")
            .build();
    /**
     * Token to use when sending CLI commands.
     */
    public static final Option TOKEN = Option
            .builder("t")
            .longOpt("access-token")
            .hasArg()
            .desc("[Optional] Access token to authenticate at server")
            .build();
    /**
     * The username for the user to manage.
     */
    public static final Option USERNAME = Option
            .builder("u")
            .longOpt("username")
            .hasArg()
            .required()
            .desc("Username")
            .build();
    /**
     * The password for the user to create.
     */
    public static final Option PASSWORD = Option
            .builder("p")
            .longOpt("password")
            .hasArg()
            .desc("[Optional] Password for the user")
            .build();
    /**
     * Signify this user will not have a password, which effectively creates a locked account.
     */
    public static final Option NO_PASSWORD = Option
            .builder()
            .longOpt("no-password")
            .desc("[Optional] Create a (locked) user account without a password.")
            .build();
    /**
     * Defines the token for a new application. If this is omitted Axon Server will generate a token.
     */
    public static final Option SET_TOKEN = Option
            .builder("T")
            .longOpt("token")
            .hasArg()
            .desc("use this token for the app")
            .build();
    /**
     * Output format for the command (text or json).
     */
    public static final Option OUTPUT = Option
            .builder("o")
            .longOpt("output")
            .hasArg()
            .desc("Output format (txt,json)")
            .build();
    /**
     * Output format for the command (text or json).
     */
    public static final Option FILE = Option
            .builder("f")
            .required()
            .longOpt("file")
            .hasArg()
            .desc("license file to upload")
            .build();

    /**
     * Extension file
     */
    public static final Option EXTENSION_FILE = Option
            .builder("f")
            .required()
            .longOpt("file")
            .hasArg()
            .desc("Jar file containing the extension bundle")
            .build();
    /**
     * YAML file containing properties
     */
    public static final Option PROPERTIES_FILE = Option
            .builder("f")
            .longOpt("file")
            .hasArg()
            .desc("YAML file with properties")
            .build();
}
