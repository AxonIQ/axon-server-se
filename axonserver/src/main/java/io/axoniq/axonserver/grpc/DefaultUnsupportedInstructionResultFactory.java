package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.exception.ErrorCode;
import org.springframework.stereotype.Component;

/**
 * Creates unsupported {@link InstructionResult} message.
 *
 * @author Milan Savic
 * @since 4.2.3
 */
@Component
public class DefaultUnsupportedInstructionResultFactory implements UnsupportedInstructionResultFactory {

    @Override
    public InstructionResult create(String instructionId, String source) {
        return InstructionResult.newBuilder()
                                .setInstructionId(instructionId)
                                .setSuccess(false)
                                .setError(ErrorMessage.newBuilder()
                                                      .setErrorCode(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode())
                                                      .addDetails("Unsupported instruction")
                                                      .setLocation(source)
                                                      .build())
                                .build();
    }
}
