package io.axoniq.axonhub.spring;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

/**
 * Created by Sara Pellegrini on 13/04/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeMessageChannel implements MessageChannel {

    private final boolean success;

    public FakeMessageChannel(boolean success) {
        this.success = success;
    }

    @Override
    public boolean send(Message<?> message) {
        return success;
    }

    @Override
    public boolean send(Message<?> message, long timeout) {
        return success;
    }
}
