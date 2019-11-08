package io.axoniq.axonserver.grpc;

import io.grpc.stub.StreamObserver;

import java.util.function.Function;

/**
 * Default implementation of {@link InstructionAckSource}.
 *
 * @param <T> the type of message to be sent
 * @author Milan Savic
 * @since 4.2.3
 */
public class DefaultInstructionAckSource<T> implements InstructionAckSource<T> {

    private final Function<InstructionAck, T> messageCreator;

    /**
     * Instantiates {@link DefaultInstructionAckSource}.
     *
     * @param messageCreator creates a message based on {@link InstructionAck}
     */
    public DefaultInstructionAckSource(Function<InstructionAck, T> messageCreator) {
        this.messageCreator = messageCreator;
    }

    @Override
    public void sendAck(String instructionId, boolean success, ErrorMessage error, StreamObserver<T> stream) {
        if (instructionId == null || instructionId.equals("")) {
            return;
        }
        InstructionAck.Builder builder = InstructionAck.newBuilder()
                                                       .setInstructionId(instructionId)
                                                       .setSuccess(success);
        if (error != null) {
            builder.setError(error);
        }

        stream.onNext(messageCreator.apply(builder.build()));
    }
}

