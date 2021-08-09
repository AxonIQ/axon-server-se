package io.axoniq.axonserver.grpc.istruction.result;

import io.axoniq.axonserver.grpc.InstructionResult;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.istruction.result.InstructionResultSource.ResultSubscriber;
import io.axoniq.axonserver.util.DaemonThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase.RESULT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Responsible to listen to all the {@link InstructionResult}s received from the client applications and to act as
 * a {@link InstructionResultSource.Factory} for all components interested in receiving a result of execution for
 * a specific instruction.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class InstructionResultSourceFactory implements InstructionResultSource.Factory {

    private static final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new DaemonThreadFactory("instruction-result"));
    /*Timeout for instruction result in seconds*/
    private int resultTimeout = 10;
    private final ConcurrentMap<String, List<ResultSubscriber>> subscribersMap = new ConcurrentHashMap<>();

    /**
     * Creates an instance of {@link InstructionResultSourceFactory} base on the {@link PlatformService}
     *
     * @param platformService used to register the new instance as a listener of the {@link InstructionResult}s
     */
    @Autowired
    public InstructionResultSourceFactory(PlatformService platformService,
                                          @Value("${axoniq.axonserver.instruction.result.timeout:10}") int resultTimeout) {
        this(resultHandler -> platformService.onInboundInstruction(
                RESULT, (clientComponent, instruction) -> resultHandler.accept(instruction.getResult())
        ), resultTimeout);
    }


    /**
     * Creates a new instance of {@link InstructionResultSourceFactory} based on the specified registration.
     *
     * @param registration the registration used to register the current instance as a listener
     *                     for all the {@link InstructionResult} received by this Axon Server node
     */
    public InstructionResultSourceFactory(Consumer<Consumer<InstructionResult>> registration,
                                          int resultTimeout) {
        registration.accept(this::on);
        this.resultTimeout = resultTimeout;
    }

    private void on(InstructionResult instructionResult) {
        subscribersMap.getOrDefault(instructionResult.getInstructionId(), Collections.emptyList())
                      .forEach(subscriber -> subscriber.onResult(instructionResult));
    }

    /**
     * {@inheritDoc}
     * Please note that the subscribable {@link InstructionResult} source that is returned by this factory dispose
     * automatically a subscription after the first {@link InstructionResult} has been received or after the timeout
     * is elapsed without any {@link InstructionResult}.
     *
     * @param instructionId the identifier of the instruction
     * @return the subscribable {@link InstructionResultSource} for the specified instruction identifier.
     */
    @Override
    public InstructionResultSource onInstructionResultFor(String instructionId) {
        return new InstructionResultSource() {

            @Override
            public void subscribe(ResultSubscriber resultSubscriber, Duration timeout) {
                Function<String, List<ResultSubscriber>> copyOnWriteArrayList = id -> new CopyOnWriteArrayList<>();
                List<ResultSubscriber> subscribers = subscribersMap.computeIfAbsent(instructionId,
                                                                                    copyOnWriteArrayList);
                subscribers.add(new TimeoutResultSubscriber(resultSubscriber, timeout, subscribers::remove));
            }

            @Override
            public Duration defaultTimeout() {
                return Duration.ofSeconds(resultTimeout);
            }
        };
    }

    private static class TimeoutResultSubscriber implements ResultSubscriber {

        private final ResultSubscriber delegate;
        private final ScheduledFuture<?> onTimeout;
        private final Consumer<TimeoutResultSubscriber> cancelSubscription;

        private TimeoutResultSubscriber(ResultSubscriber resultSubscriber, Duration timeout,
                                        Consumer<TimeoutResultSubscriber> cancelSubscription) {
            this.delegate = resultSubscriber;
            this.cancelSubscription = cancelSubscription;
            this.onTimeout = scheduledExecutorService.schedule(this::onTimeout, timeout.toMillis(), MILLISECONDS);
        }

        @Override
        public void onResult(InstructionResult result) {
            if (onTimeout.cancel(false)) {
                delegate.onResult(result);
                cancelSubscription.accept(this);
            }
        }

        @Override
        public void onTimeout() {
            cancelSubscription.accept(this);
            delegate.onTimeout();
        }
    }
}
