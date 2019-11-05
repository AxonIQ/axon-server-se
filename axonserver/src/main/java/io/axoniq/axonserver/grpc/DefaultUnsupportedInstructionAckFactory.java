package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.exception.ErrorCode;
import org.springframework.stereotype.Component;

/**
 * Creates unsupported {@link InstructionAck} message.
 *
 * @author Milan Savic
 * @since 4.2.3
 */
@Component
public class DefaultUnsupportedInstructionAckFactory implements UnsupportedInstructionAckFactory {

    @Override
    public InstructionAck create(String instructionId, String source) {
        return InstructionAck.newBuilder()
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
