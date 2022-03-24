/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;

import java.util.List;
import java.util.Optional;

/**
 * Utility class to retrieve values of processing instructions from incoming messages
 *
 * @author Marc Gathier
 */
public abstract class ProcessingInstructionHelper {

    private ProcessingInstructionHelper() {
        // utility class
    }

    public static String getProcessingInstructionString(List<ProcessingInstruction> processingInstructions,
                                                        ProcessingKey key, String defaultValue) {
        for (ProcessingInstruction pi : processingInstructions) {
            if (key.equals(pi.getKey())) {
                return pi.getValue().getTextValue();
            }
        }
        return defaultValue;
    }

    public static long getProcessingInstructionNumber(List<ProcessingInstruction> processingInstructions, ProcessingKey key) {
        return getProcessingInstructionNumber(processingInstructions, key, 0L);
    }

    public static long getProcessingInstructionNumber(List<ProcessingInstruction> processingInstructions,
                                                      ProcessingKey key, long defaultValue) {
        for (ProcessingInstruction pi : processingInstructions) {
            if (key.equals(pi.getKey())) {
                return pi.getValue().getNumberValue();
            }
        }
        return defaultValue;
    }

    public static boolean clientSupportsQueryStreaming(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionBoolean(processingInstructions,
                                               ProcessingKey.CLIENT_SUPPORTS_STREAMING).orElse(false);
    }

    public static Optional<Boolean> getProcessingInstructionBoolean(List<ProcessingInstruction> instructions,
                                                                    ProcessingKey key) {
        return instructions.stream()
                           .filter(instruction -> key.equals(instruction.getKey()))
                           .map(instruction -> instruction.getValue().getBooleanValue())
                           .findFirst();
    }

    public static long priority(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionNumber(processingInstructions, ProcessingKey.PRIORITY);
    }

    public static long timeout(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionNumber(processingInstructions, ProcessingKey.TIMEOUT, 5000);
    }

    public static int numberOfResults(List<ProcessingInstruction> processingInstructions) {
        return (int) getProcessingInstructionNumber(processingInstructions, ProcessingKey.NR_OF_RESULTS);
    }

    public static String routingKey(List<ProcessingInstruction> processingInstructions, String defaultRoutingKey) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.ROUTING_KEY, defaultRoutingKey);
    }

    public static ProcessingInstruction timeout(long timeout) {
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.TIMEOUT)
                                    .setValue(MetaDataValue.newBuilder().setNumberValue(timeout))
                                    .build();
    }

    public static ProcessingInstruction numberOfResults(long numberOfResults) {
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.NR_OF_RESULTS)
                                    .setValue(MetaDataValue.newBuilder().setNumberValue(numberOfResults))
                                    .build();
    }

    public static ProcessingInstruction routingKey(String routingKey) {
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.ROUTING_KEY)
                                    .setValue(MetaDataValue.newBuilder().setTextValue(routingKey))
                                    .build();
    }

    public static ProcessingInstruction priority(long priority) {
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.PRIORITY)
                                    .setValue(MetaDataValue.newBuilder().setNumberValue(priority))
                                    .build();
    }
}
