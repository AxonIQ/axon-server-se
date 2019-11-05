package io.axoniq.axonserver.grpc;

/**
 * Defines a contract for implementing unsupported {@link InstructionAck} message.
 *
 * @author Milan Savic
 * @since 4.2.3
 */
@FunctionalInterface
public interface UnsupportedInstructionAckFactory {

    /**
     * Creates unsupported instruction result based on {@code instructionId} and {@code source}.
     *
     * @param instructionId identifier of unsupported instruction
     * @param source        the source where the instruction is not supported
     * @return unsupported {@link InstructionAck}
     */
    InstructionAck create(String instructionId, String source);
}
