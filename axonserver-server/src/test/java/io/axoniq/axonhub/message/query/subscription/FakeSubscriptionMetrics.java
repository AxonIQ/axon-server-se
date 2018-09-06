package io.axoniq.axonhub.message.query.subscription;

import io.axoniq.axonhub.serializer.Media;

/**
 * Created by Sara Pellegrini on 19/06/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeSubscriptionMetrics implements SubscriptionMetrics{

    private final long totalCount;
    private final long activesCount;
    private final long updatesCount;

    public FakeSubscriptionMetrics(long totalCount, long activesCount, long updatesCount) {
        this.totalCount = totalCount;
        this.activesCount = activesCount;
        this.updatesCount = updatesCount;
    }


    @Override
    public Long totalCount() {
        return totalCount;
    }

    @Override
    public Long activesCount() {
        return activesCount;
    }

    @Override
    public Long updatesCount() {
        return updatesCount;
    }


    @Override
    public void printOn(Media media) {

    }
}