package io.axoniq.axonserver.metric;

import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;

/**
 * @author Sara Pellegrini
 * @since 4.5
 */
public class TimeMeasuredPublisher<T> implements Publisher<T> {

    private final Publisher<T> delegatePublisher;
    private final Timer subscriptionTimer;

    public TimeMeasuredPublisher(Publisher<T> delegatePublisher, Timer subscriptionTimer) {
        this.delegatePublisher = delegatePublisher;
        this.subscriptionTimer = subscriptionTimer;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        TimeSubscriber subscriber = new TimeSubscriber(s);
        delegatePublisher.subscribe(subscriber);
    }

    private final class TimeSubscriber implements Subscriber<T> {

        private final Subscriber<? super T> delegateSubscriber;
        private final long before = System.currentTimeMillis();

        private TimeSubscriber(Subscriber<? super T> delegateSubscriber) {
            this.delegateSubscriber = delegateSubscriber;
        }


        @Override
        public void onSubscribe(Subscription s) {
            delegateSubscriber.onSubscribe(s);
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
            subscriptionTimer.record(System.currentTimeMillis() - before, TimeUnit.MILLISECONDS);
        }
    }
}
