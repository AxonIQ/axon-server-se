package io.axoniq.axonserver.grpc.istruction.result;

import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.InstructionResult;

import java.time.Duration;
import java.util.function.Consumer;

import static io.axoniq.axonserver.exception.ErrorCode.INSTRUCTION_RESULT_TIMEOUT;

/**
 * Subscribable source of {@link InstructionResult} that has been received from a client application.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public interface InstructionResultSource {

    /**
     * The default timeout for an {@link InstructionResult} to be notified. The value is 10 seconds.
     */
    default Duration defaultTimeout() {
        return Duration.ofSeconds(10);
    }

    /**
     * Subscribes the specified handlers to this source of {@link InstructionResult}.
     * In case of successful result the {@code onSuccess} {@link Runnable} is executed.
     * In case of a failure or after the {@link InstructionResultSource#defaultTimeout()} is elapsed without
     * any {@link InstructionResult}, the {@code onFailureOrTimeout} {@link Consumer} is invoked.
     *
     * @param onSuccess          invoked in case of successful {@link InstructionResult}
     * @param onFailureOrTimeout handles the {@link ErrorMessage} in case failure {@link InstructionResult} or
     *                           when the timeout elapsed without any {@link InstructionResult}
     */
    default void subscribe(Runnable onSuccess,
                           Consumer<ErrorMessage> onFailureOrTimeout) {
        subscribe(onSuccess, onFailureOrTimeout, defaultTimeout());
    }

    /**
     * Subscribes the specified handlers to this source of {@link InstructionResult}.
     * In case of successful result the {@code onSuccess} {@link Runnable} is executed.
     * In case of a failure or after the specified timeout is elapsed without any {@link InstructionResult},
     * the {@code onFailureOrTimeout} {@link Consumer} is invoked.
     *
     * @param onSuccess          invoked in case of successful {@link InstructionResult}
     * @param onFailureOrTimeout handles the {@link ErrorMessage} in case failure {@link InstructionResult} or
     *                           when the timeout elapsed without any {@link InstructionResult}
     * @param timeout            the time without any {@link InstructionResult} received after which the
     *                           {@code onFailureOrTimeout} {@link Consumer} is executed
     */
    default void subscribe(Runnable onSuccess,
                           Consumer<ErrorMessage> onFailureOrTimeout,
                           Duration timeout) {
        subscribe(onSuccess, onFailureOrTimeout, onFailureOrTimeout, timeout);
    }

    /**
     * Subscribes the specified handlers to this source of {@link InstructionResult}.
     * In case of successful result the {@code onSuccess} {@link Runnable} is executed.
     * In case of a failure the {@code onFailure} {@link Consumer} is invoked.
     * After the {@link InstructionResultSource#defaultTimeout()} is elapsed without any {@link InstructionResult},
     * the {@code onTimeout} {@link Consumer} is invoked.
     *
     * @param onSuccess invoked in case of successful {@link InstructionResult}
     * @param onFailure handles the {@link ErrorMessage} in case failure {@link InstructionResult}
     * @param onTimeout handles the {@link ErrorMessage} when the timeout elapsed without any {@link InstructionResult}
     */
    default void subscribe(Runnable onSuccess,
                           Consumer<ErrorMessage> onFailure,
                           Consumer<ErrorMessage> onTimeout) {
        subscribe(onSuccess, onFailure, onTimeout, defaultTimeout());
    }

    /**
     * Subscribes the specified handlers to this source of {@link InstructionResult}.
     * In case of successful result the {@code onSuccess} {@link Runnable} is executed.
     * In case of a failure the {@code onFailure} {@link Consumer} is invoked.
     * After the specified timeout is elapsed without any {@link InstructionResult},
     * the {@code onTimeout} {@link Consumer} is invoked.
     *
     * @param onSuccess invoked in case of successful {@link InstructionResult}
     * @param onFailure handles the {@link ErrorMessage} in case failure {@link InstructionResult}
     * @param onTimeout handles the {@link ErrorMessage} when the timeout elapsed without any {@link InstructionResult}
     * @param timeout   the time without any {@link InstructionResult} received after which the {@code onTimeout}
     *                  {@link Consumer} is executed
     */
    default void subscribe(Runnable onSuccess,
                           Consumer<ErrorMessage> onFailure,
                           Consumer<ErrorMessage> onTimeout,
                           Duration timeout) {
        subscribe(new ResultSubscriber() {
            @Override
            public void onResult(InstructionResult result) {
                if (result.getSuccess()) {
                    onSuccess.run();
                } else {
                    onFailure.accept(result.getError());
                }
            }

            @Override
            public void onTimeout() {
                onTimeout.accept(ErrorMessage
                                         .newBuilder()
                                         .setErrorCode(INSTRUCTION_RESULT_TIMEOUT.getCode())
                                         .setMessage("Instruction Result Timeout")
                                         .build());
            }
        }, timeout);
    }

    /**
     * Subscribes the specified {@link Consumer} to this source of {@link InstructionResult}. After the
     * {@link InstructionResultSource#defaultTimeout()} is elapsed without any {@link InstructionResult},
     * the {@code onTimeout} {@link Runnable} is invoked.
     *
     * @param onResult  the consumer of the {@link InstructionResult}
     * @param onTimeout the function that is invoked when the timeout elapsed without any {@link InstructionResult}
     */
    default void subscribe(Consumer<InstructionResult> onResult,
                           Runnable onTimeout) {
        subscribe(onResult, onTimeout, defaultTimeout());
    }

    /**
     * Subscribes the specified {@link Consumer} to this source of {@link InstructionResult}. After the
     * specified timeout is elapsed without any {@link InstructionResult}, the {@code onTimeout} {@link Runnable}
     * is invoked.
     *
     * @param onResult  the consumer of the {@link InstructionResult}
     * @param onTimeout the function that is invoked when the timeout elapsed without any {@link InstructionResult}
     * @param timeout   the time without any {@link InstructionResult} received after which the {@code onTimeout}
     *                  {@link Runnable} is executed
     */
    default void subscribe(Consumer<InstructionResult> onResult,
                           Runnable onTimeout,
                           Duration timeout) {
        subscribe(new ResultSubscriber() {
            @Override
            public void onResult(InstructionResult result) {
                onResult.accept(result);
            }

            @Override
            public void onTimeout() {
                onTimeout.run();
            }
        }, timeout);
    }

    /**
     * Subscribes the specified {@link ResultSubscriber} to this source of {@link InstructionResult}. After the
     * {@link InstructionResultSource#defaultTimeout()} is elapsed without any {@link InstructionResult}, the
     * subscriber's {@link ResultSubscriber#onTimeout()} is invoked.
     *
     * @param resultSubscriber the subscriber of the {@link InstructionResult}
     */
    default void subscribe(ResultSubscriber resultSubscriber) {
        subscribe(resultSubscriber, defaultTimeout());
    }

    /**
     * Subscribes the specified {@link ResultSubscriber} to this source, using the specified timeout.
     *
     * @param resultSubscriber the subscriber of the {@link InstructionResult}
     * @param timeout          the time without any {@link InstructionResult} received after which
     *                         the {@link ResultSubscriber#onTimeout()} function is triggered
     */
    void subscribe(ResultSubscriber resultSubscriber, Duration timeout);

    /**
     * Subscriber for the result of the execution of an instruction sent from Axon Server to a client application.
     */
    interface ResultSubscriber {

        /**
         * Handle the {@link InstructionResult} for the subscribed instruction
         *
         * @param result the result of the execution of the instruction sent from the client to Axon Server
         */
        void onResult(InstructionResult result);

        /**
         * Handle the case of timeout elapsed without any {@link InstructionResult} has been received from the client
         * application. This can happen for many reasons (network latency, client disconnection, heavy load of client)
         * and it is still possible to receive the result after timeout.
         * The implementation of this method should cancel the subscription if the subscriber is not interested in a
         * result after the timeout is elapsed.
         */
        void onTimeout();
    }

    /**
     * Factory for the {@link InstructionResultSource} for the specified instruction identifier.
     */
    interface Factory {

        /**
         * Returns the subscribable {@link InstructionResultSource} for the specified instruction identifier.
         *
         * @param instructionId the identifier of the instruction
         * @return the subscribable {@link InstructionResultSource} for the specified instruction identifier.
         */
        InstructionResultSource onInstructionResultFor(String instructionId);
    }
}
