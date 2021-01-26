package io.axoniq.axonserver.localstorage;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author Milan Savic
 */
public class CountingPublisher<T> implements Publisher<T> {

    private final Publisher<T> delegate;
    private final Consumer<Integer> doOnComplete;

    public CountingPublisher(Publisher<T> delegate) {
        this(delegate, i -> {});
    }

    public CountingPublisher(Publisher<T> delegate, Consumer<Integer> doOnComplete) {
        this.delegate = delegate;
        this.doOnComplete = doOnComplete;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        CountingSubscriber<? super T> countingSubscriber = new CountingSubscriber<>(s);
        delegate.subscribe(countingSubscriber);
    }

    private class CountingSubscriber<T> implements Subscriber<T> {

        private final AtomicInteger counter = new AtomicInteger();
        private final Subscriber<T> delegate;

        private CountingSubscriber(Subscriber<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSubscribe(Subscription s) {
            delegate.onSubscribe(s);
        }

        @Override
        public void onNext(T t) {
            counter.incrementAndGet();
            delegate.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            delegate.onError(t);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
            doOnComplete.accept(counter.get());
        }
    }
}
