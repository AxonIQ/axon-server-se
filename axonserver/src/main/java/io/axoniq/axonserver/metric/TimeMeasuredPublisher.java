package io.axoniq.axonserver.metric;

import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.5
 */
public class TimeMeasuredPublisher<T> implements Publisher<T> {

    private final Publisher<T> delegatePublisher;
    private final Timer subscriptionTimer;
    private final LongSupplier timeSupplier;

    public TimeMeasuredPublisher(Publisher<T> delegatePublisher, Timer subscriptionTimer) {
        this(delegatePublisher, subscriptionTimer, System::currentTimeMillis);
    }

    public TimeMeasuredPublisher(Publisher<T> delegatePublisher, Timer subscriptionTimer, LongSupplier timeSupplier) {
        this.delegatePublisher = delegatePublisher;
        this.subscriptionTimer = subscriptionTimer;
        this.timeSupplier = timeSupplier;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        TimeSubscriber subscriber = new TimeSubscriber(s);
        delegatePublisher.subscribe(subscriber);
    }

    private final class TimeSubscriber implements Subscriber<T> {

        private final Subscriber<? super T> delegateSubscriber;
        private final AtomicLong before = new AtomicLong();

        private TimeSubscriber(Subscriber<? super T> delegateSubscriber) {
            this.delegateSubscriber = delegateSubscriber;
        }

        @Override
        public void onSubscribe(Subscription s) {
            delegateSubscriber.onSubscribe(s);
            before.set(timeSupplier.getAsLong());
        }

        @Override
        public void onNext(T o) {
            delegateSubscriber.onNext(o);
        }

        @Override
        public void onError(Throwable t) {
            delegateSubscriber.onError(t);
        }

        @Override
        public void onComplete() {
            delegateSubscriber.onComplete();
            subscriptionTimer.record(timeSupplier.getAsLong() - before.get(), TimeUnit.MILLISECONDS);
        }
    }
}
