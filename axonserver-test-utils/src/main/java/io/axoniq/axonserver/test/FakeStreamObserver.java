package io.axoniq.axonserver.test;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fake implementation of {@link StreamObserver} useful for testing.
 *
 * @author Sara Pellegrini
 */
public class FakeStreamObserver<M> extends ServerCallStreamObserver<M> {

    private List<M> values = new LinkedList<>();
    private List<Throwable> errors = new LinkedList<>();
    private int completedCount = 0;
    private final AtomicBoolean isReady = new AtomicBoolean(false);

    private Runnable onReadyHandler = () -> {};

    @Override
    public void onNext(M value) {
        values.add(value);
    }

    @Override
    public void onError(Throwable t) {
        errors.add(t);
    }

    @Override
    public void onCompleted() {
        completedCount++;
    }

    public List<M> values() {
        return values;
    }

    public List<Throwable> errors() {
        return errors;
    }

    public int completedCount() {
        return completedCount;
    }

    public void setIsReady(boolean value) {
        boolean oldValue = isReady.get();
        isReady.set(value);

        if (value && !oldValue) {
            onReadyHandler.run();
        }
    }

    @Override
    public boolean isReady() {
        return isReady.get();
    }

    @Override
    public void setOnReadyHandler(Runnable runnable) {
        this.onReadyHandler = runnable;
    }

    @Override
    public void disableAutoInboundFlowControl() {

    }

    @Override
    public void request(int i) {

    }

    @Override
    public void setMessageCompression(boolean b) {

    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public void setOnCancelHandler(Runnable runnable) {

    }

    @Override
    public void setCompression(String s) {

    }
}
