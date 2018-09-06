package io.axoniq.axonhub.util;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: marc
 */
public class CountingStreamObserver<T> implements StreamObserver<T> {
    public int count;
    public Throwable error;
    public boolean completed;
    public List<T> responseList = new ArrayList<>();

    @Override
    public void onNext(T queryResponse) {
        responseList.add(queryResponse);
        count++;
    }

    @Override
    public void onError(Throwable throwable) {
        error = throwable;
    }

    @Override
    public void onCompleted() {
        completed = true;
    }
}
