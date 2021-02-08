/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marc Gathier
 */
public class CommandDispatcher {

    @FunctionalInterface
    interface CommandProcessor {

        void process(String[] strings) throws Exception;
    }

    enum Group {
        APPLICATION(true, false, "Applications"),
        REPLICATIONGROUP(true, false, "Replication Group"),
        CLUSTER(true, false, "Cluster"),
        CONTEXT(true, false, "Context"),
        METRICS(true, true, "Metrics"),
        USER(true, true, "Users"),
        EXTENSIONS(true, true, "Extensions"),
        OTHER(false, true, "Other"),
        ;

        private final boolean eeSupport;
        private final boolean seSupport;
        private final String description;

        Group(boolean eeSupport, boolean seSupport, String description) {
            this.eeSupport = eeSupport;
            this.seSupport = seSupport;
            this.description = description;
        }

        @Override
        public String toString() {
            if (!seSupport) {
                return description + " [Enterprise edition only]";
            }
            if (!eeSupport) {
                return description + " [Standard edition only]";
            }
            return description;
        }
    }

    static class CommandInformation {

        CommandInformation(Group group, CommandProcessor commandProcessor) {
            this.group = group;
            this.commandProcessor = commandProcessor;
        }

        public void process(String[] args) throws Exception {
            commandProcessor.process(args);
        }

        final Group group;
        final CommandProcessor commandProcessor;
    }

    private static final Map<String, CommandInformation> executorMap = new HashMap<>();

    static {
        executorMap.put("cluster", new CommandInformation(Group.CLUSTER, Cluster::run));
        executorMap.put("delete-application", new CommandInformation(Group.APPLICATION, DeleteApplication::run));
        executorMap.put("register-application", new CommandInformation(Group.APPLICATION, RegisterApplication::run));
        executorMap.put("applications", new CommandInformation(Group.APPLICATION, ListApplications::run));
        executorMap.put("register-node", new CommandInformation(Group.CLUSTER, RegisterNode::run));
        executorMap.put("unregister-node", new CommandInformation(Group.CLUSTER, UnregisterNode::run));
        executorMap.put("register-context", new CommandInformation(Group.CONTEXT, RegisterContext::run));
        executorMap.put("delete-context", new CommandInformation(Group.CONTEXT, DeleteContext::run));
        executorMap.put("add-node-to-context", new CommandInformation(Group.CONTEXT, AddNodeToContext::run));
        executorMap.put("delete-node-from-context", new CommandInformation(Group.CONTEXT, DeleteNodeFromContext::run));
        executorMap.put("contexts", new CommandInformation(Group.CONTEXT, ListContexts::run));
        executorMap.put("register-replication-group",
                        new CommandInformation(Group.REPLICATIONGROUP, RegisterReplicationGroup::run));
        executorMap.put("delete-replication-group",
                        new CommandInformation(Group.REPLICATIONGROUP, DeleteReplicationGroup::run));
        executorMap.put("add-node-to-replication-group",
                        new CommandInformation(Group.REPLICATIONGROUP, AddNodeToReplicationGroup::run));
        executorMap.put("delete-node-from-replication-group",
                        new CommandInformation(Group.REPLICATIONGROUP, DeleteNodeFromReplicationGroup::run));
        executorMap.put("replication-groups",
                        new CommandInformation(Group.REPLICATIONGROUP, ListReplicationGroups::run));
        executorMap.put("register-user", new CommandInformation(Group.USER, RegisterUser::run));
        executorMap.put("delete-user", new CommandInformation(Group.USER, DeleteUser::run));
        executorMap.put("users", new CommandInformation(Group.USER, ListUsers::run));
        executorMap.put("metrics", new CommandInformation(Group.METRICS, Metrics::run));
        executorMap.put("init-cluster", new CommandInformation(Group.CLUSTER, InitNode::run));
        executorMap.put("purge-events", new CommandInformation(Group.OTHER, DeleteEvents::run));
        executorMap.put("update-license", new CommandInformation(Group.CLUSTER, UpdateLicense::run));
        executorMap.put("upload-extension", new CommandInformation(Group.EXTENSIONS, UploadExtension::run));
        executorMap.put("configure-extension", new CommandInformation(Group.EXTENSIONS, ConfigureExtension::run));
        executorMap.put("activate-extension",
                        new CommandInformation(Group.EXTENSIONS, args -> ActivateExtension.run(args, true)));
        executorMap.put("pause-extension",
                        new CommandInformation(Group.EXTENSIONS, args -> ActivateExtension.run(args, false)));
        executorMap.put("unregister-extension", new CommandInformation(Group.EXTENSIONS, UnregisterExtension::run));
        executorMap.put("delete-extension", new CommandInformation(Group.EXTENSIONS, DeleteExtension::run));
        executorMap.put("extensions", new CommandInformation(Group.EXTENSIONS, ListExtensions::run));
    }

    public static void main(String[] args)  {
        if( args.length == 0) {
            System.err.println("No command specified. Valid commands: ");
            usage();

            System.exit(1);
        }

        CommandInformation executor = executorMap.get(args[0]);
        if( executor != null) {
            try {
                executor.process(args);
            } catch (CommandExecutionException ex) {
                System.err.println("Error processing command '" + args[0] + "' on '"+ ex.getUrl() + "': " + ex.getMessage());
                System.exit(ex.getErrorCode());
            } catch (Exception ex) {
                System.err.println("Error processing command '" + args[0] + "': " + ex.getMessage());
                System.exit(1);
            }
        } else {
            System.err.println("Invalid command specified: " + args[0] + ". Valid commands: ");
            usage();
            System.exit(1);
        }
    }

    private static void usage() {
        for (Group value : Group.values()) {
            System.err.println(value);
            executorMap.entrySet()
                       .stream()
                       .filter(e -> e.getValue().group.equals(value))
                       .sorted(Map.Entry.comparingByKey())
                       .forEach(e -> System.err.println("  " + e.getKey()));
        }
    }
}
