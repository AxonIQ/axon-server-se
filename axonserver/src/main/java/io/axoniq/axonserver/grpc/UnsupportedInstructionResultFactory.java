package io.axoniq.axonserver.grpc;

/**
 * Defines a contract for implementing unsupported {@link InstructionResult} message.
 *
 * @author Milan Savic
 * @since 4.2.3
 */
@FunctionalInterface
public interface UnsupportedInstructionResultFactory {

    /**
     * Creates unsupported instruction result based on {@code instructionId} and {@code source}.
     *
     * @param instructionId identifier of unsupported instruction
     * @param source        the source where the instruction is not supported
     * @return unsupported {@link InstructionResult}
     */
    InstructionResult create(String instructionId, String source);
}
