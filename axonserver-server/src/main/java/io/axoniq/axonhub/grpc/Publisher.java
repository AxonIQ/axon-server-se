package io.axoniq.axonhub.grpc;

/**
 * Created by Sara Pellegrini on 04/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface Publisher<T> {

    void publish(T message);

}
