package io.axoniq.axonserver;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;

import java.util.List;

/**
 * Author: marc
 */
public interface ProcessingInstructionHelper {

    static String getProcessingInstructionString(List<ProcessingInstruction> processingInstructions, ProcessingKey key) {
        return processingInstructions.stream()
                .filter(pi -> key.equals(pi.getKey()))
                .map(pi -> pi.getValue().getTextValue())
                .findFirst()
                .orElse(null);
    }

    static long getProcessingInstructionNumber(List<ProcessingInstruction> processingInstructions, ProcessingKey key) {
        return processingInstructions.stream()
                .filter(pi -> key.equals(pi.getKey()))
                .map(pi -> pi.getValue().getNumberValue())
                .findFirst()
                .orElse(0L);
    }

    static long priority(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionNumber(processingInstructions, ProcessingKey.PRIORITY);
    }

    static long timeout(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionNumber(processingInstructions, ProcessingKey.TIMEOUT);
    }

    static int numberOfResults(List<ProcessingInstruction> processingInstructions) {
        return (int)getProcessingInstructionNumber(processingInstructions, ProcessingKey.NR_OF_RESULTS);
    }

    static String targetClient(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.TARGET_CLIENT);
    }

    static String context(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.CONTEXT);
    }

    static String routingKey(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.ROUTING_KEY);
    }

    static String originalMessageId(List<ProcessingInstruction> processingInstructions) {
        return getProcessingInstructionString(processingInstructions, ProcessingKey.ORIGINAL_MESSAGE_ID);
    }

    static ProcessingInstruction originalMessageId(String messageIdentifier) {
        return ProcessingInstruction.newBuilder()
                .setKey(ProcessingKey.ORIGINAL_MESSAGE_ID)
                .setValue(MetaDataValue.newBuilder().setTextValue(messageIdentifier))
                .build();
    }

    static ProcessingInstruction targetClient(String targetClient) {
        return ProcessingInstruction.newBuilder()
                .setKey(ProcessingKey.TARGET_CLIENT)
                .setValue(MetaDataValue.newBuilder().setTextValue(targetClient))
                .build();
    }

    static ProcessingInstruction context(String context) {
        return ProcessingInstruction.newBuilder()
                .setKey(ProcessingKey.CONTEXT)
                .setValue(MetaDataValue.newBuilder().setTextValue(context))
                .build();
    }

    static ProcessingInstruction timeout(long timeout) {
        return ProcessingInstruction.newBuilder()
                .setKey(ProcessingKey.TIMEOUT)
                .setValue(MetaDataValue.newBuilder().setNumberValue(timeout))
                .build();
    }

    static ProcessingInstruction routingKey(String routingKey) {
        return ProcessingInstruction.newBuilder()
                .setKey(ProcessingKey.ROUTING_KEY)
                .setValue(MetaDataValue.newBuilder().setTextValue(routingKey))
                .build();
    }
}
