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

/**
 * Utility class to retrieve values of processing instructions from incoming messages
 *
 * @author Marc Gathier
 */
public abstract class ProcessingInstructionHelper {

    private ProcessingInstructionHelper() {
        // utility class
    }

    public static String getProcessingInstructionString(List<ProcessingInstruction> processingInstructions, ProcessingKey key) {
        return processingInstructions.stream()
                                     .filter(pi -> key.equals(pi.getKey()))
                                     .map(pi -> pi.getValue().getTextValue())
                                     .findFirst()
                                     .orElse(null);
    }

    public static long getProcessingInstructionNumber(List<ProcessingInstruction> processingInstructions, ProcessingKey key) {
        return processingInstructions.stream()
                                     .filter(pi -> key.equals(pi.getKey()))
                                     .map(pi -> pi.getValue().getNumberValue())
                                     .findFirst()
                                     .orElse(0L);
    }

    public static long priority(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionNumber(processingInstructions, ProcessingKey.PRIORITY);
    }

    public static long timeout(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionNumber(processingInstructions, ProcessingKey.TIMEOUT);
    }

    public static int numberOfResults(List<ProcessingInstruction> processingInstructions) {
        return (int) getProcessingInstructionNumber(processingInstructions, ProcessingKey.NR_OF_RESULTS);
    }

    public static String targetClient(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.TARGET_CLIENT);
    }

    public static String context(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.CONTEXT);
    }

    public static String routingKey(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.ROUTING_KEY);
    }

    public static String originalMessageId(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.ORIGINAL_MESSAGE_ID);
    }

    public static ProcessingInstruction originalMessageId(String messageIdentifier) {
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.ORIGINAL_MESSAGE_ID)
                                    .setValue(MetaDataValue.newBuilder().setTextValue(messageIdentifier))
                                    .build();
    }

    public static ProcessingInstruction targetClient(String targetClient) {
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.TARGET_CLIENT)
                                    .setValue(MetaDataValue.newBuilder().setTextValue(targetClient))
                                    .build();
    }

    public static ProcessingInstruction context(String context) {
        return ProcessingInstruction.newBuilder()
                                    .setKey(ProcessingKey.CONTEXT)
                                    .setValue(MetaDataValue.newBuilder().setTextValue(context))
                                    .build();
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
