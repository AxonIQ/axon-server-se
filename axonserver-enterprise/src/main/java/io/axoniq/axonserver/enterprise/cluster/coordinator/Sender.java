package io.axoniq.axonserver.enterprise.cluster.coordinator;

/**
 * Created by Sara Pellegrini on 23/08/2018.
 * sara.pellegrini@gmail.com
 */
public interface Sender<Message, Destination, Callback> {

    void send(Message message, Destination destination, Callback callback);

}
