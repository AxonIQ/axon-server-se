package io.axoniq.axonserver.enterprise.cluster.coordinator;

import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.grpc.Confirmation;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 23/08/2018.
 * sara.pellegrini@gmail.com
 */
public class ConfirmationTarget implements StreamObserver<Confirmation> {

    private final Supplier<String> nodeName;

    private final AtomicBoolean approved;

    private final AtomicInteger responseCount;

    private final CountDownLatch countdownLatch;

    private final Logger logger = LoggerFactory.getLogger(ConfirmationTarget.class);

    public ConfirmationTarget(Supplier<String> nodeName,
                              AtomicBoolean approved,
                              AtomicInteger responseCount,
                              CountDownLatch countdownLatch) {
        this.nodeName = nodeName;
        this.approved = approved;
        this.responseCount = responseCount;
        this.countdownLatch = countdownLatch;
    }

    @Override
    public void onNext(Confirmation confirmation) {
        approved.set(approved.get() && confirmation.getSuccess());
        responseCount.incrementAndGet();
        countdownLatch.countDown();
    }

    @Override
    public void onError(Throwable throwable) {
        countdownLatch.countDown();
        ManagedChannelHelper.checkShutdownNeeded(nodeName.get(), throwable);
        logger.debug("Error while requesting to become a coordinator", throwable);
    }

    @Override
    public void onCompleted() {
        //do nothing
    }
}
