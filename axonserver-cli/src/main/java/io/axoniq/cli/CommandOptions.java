package io.axoniq.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.PatternOptionBuilder;

/**
 * Author: marc
 */
public class CommandOptions {
    public static final Option ADDRESS = Option.builder("S").hasArg().longOpt("server").desc("Server to send command to (default http://localhost:8024)")
                                               .build();
    public static final Option APPLICATION = Option.builder("a").hasArg().longOpt("application").required().desc("Name of the application").build();
    public static final Option ROLES = Option.builder("r").hasArgs().valueSeparator(',').longOpt("roles").desc("[Optional - Enterprise Edition only] roles for the application (READ, WRITE, ADMIN)").build();
    public static final Option DESCRIPTION = Option.builder("d").hasArgs().longOpt("description").desc("[Optional] Description of the application").build();
    public static final Option NODENAME = Option.builder("n").longOpt("node").required().hasArg().desc("Name of the node").build();
    public static final Option CONTEXT = Option.builder("c").longOpt("context").required().hasArg().desc("Name of the context").build();
    public static final Option NODES = Option.builder("n").hasArgs().valueSeparator(',').longOpt("nodes").desc("[Optional - Enterprise Edition only] member nodes for context").build();
    public static final Option INTERNALHOST = Option.builder("h").longOpt("internal-host").desc("Internal hostname of the node").required().hasArg().build();
    public static final Option INTERNALPORT = Option.builder("p").longOpt("internal-port").desc("Internal port of the node (default 8224)").type(PatternOptionBuilder.NUMBER_VALUE).hasArg().build();
    public static final Option TOKEN = Option.builder("t").longOpt("access-token").desc("[Optional] Access token to authenticate at server").hasArg().build();
    public static final Option USERNAME = Option.builder("u").longOpt("username").required().desc("Username").hasArg().build();
    public static final Option PASSWORD = Option.builder("p").longOpt("password").desc("[Optional] Password for the user").hasArg().build();
    public static final Option SET_TOKEN = Option.builder("T").longOpt("token").desc("use this token for the app").hasArg().build();
    public static final Option CONTEXTS = Option.builder("c").longOpt("contexts").desc("contexts to connect to").hasArgs().valueSeparator(',').build();
    public static final Option NO_CONTEXT = Option.builder("e").longOpt("nocontexts").desc("do not connect to contexts").build();
}
