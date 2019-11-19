package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.exception.ErrorCode;
import io.grpc.stub.StreamObserver;

/**
 * Responsible for sending instruction acknowledgements.
 *
 * @param <T> the type of message to be sent
 * @author Milan Savic
 * @since 4.2.3
 */
@FunctionalInterface
public interface InstructionAckSource<T> {

    /**
     * Sends successful acknowledgement.
     *
     * @param instructionId identifier of successful instruction. If {@code null}, acknowledgement will not be sent.
     * @param stream        the stream for sending acknowledgements
     */
    default void sendSuccessfulAck(String instructionId, StreamObserver<T> stream) {
        sendAck(instructionId, true, null, stream);
    }

    /**
     * Sends acknowledgement of unsupported instruction.
     *
     * @param instructionId identifier of successful instruction. If {@code null}, acknowledgement will not be sent.
     * @param source        the location where error occurred
     * @param stream        the stream for sending acknowledgements
     */
    default void sendUnsupportedInstruction(String instructionId, String source, StreamObserver<T> stream) {
        ErrorMessage unsupportedInstruction = ErrorMessage.newBuilder()
                                                          .setErrorCode(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode())
                                                          .setLocation(source)
                                                          .addDetails("Unsupported instruction")
                                                          .build();
        sendUnsuccessfulAck(instructionId, unsupportedInstruction, stream);
    }

    /**
     * Sends unsuccessful acknowledgement.
     *
     * @param instructionId identifier of successful instruction. If {@code null}, acknowledgement will not be sent.
     * @param error         the cause of the error
     * @param stream        the stream for sending acknowledgements
     */
    default void sendUnsuccessfulAck(String instructionId, ErrorMessage error, StreamObserver<T> stream) {
        sendAck(instructionId, false, error, stream);
    }

    /**
     * Sends an acknowledgement.
     *
     * @param instructionId identifier of successful instruction. If {@code null}, acknowledgement will not be sent.
     * @param success       {@code true} if acknowledgement is successful, {@code false} otherwise
     * @param error         the cause of the error. Provide {@code null} for successful acknowledgement
     * @param stream        the stream for sending acknowledgements
     */
    void sendAck(String instructionId, boolean success, ErrorMessage error, StreamObserver<T> stream);
}
