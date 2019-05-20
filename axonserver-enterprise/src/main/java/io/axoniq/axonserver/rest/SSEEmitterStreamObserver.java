package io.axoniq.axonserver.rest;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.function.Function;

/**
 * A {@link StreamObserver} that maps the received messages and send them through a {@link SseEmitter}
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class SSEEmitterStreamObserver<M> implements StreamObserver<M> {

    private final Logger logger = LoggerFactory.getLogger(SSEEmitterStreamObserver.class);
    private final SseEmitter sseEmitter;
    private final Function<M, Object> mapping;

    public SSEEmitterStreamObserver(SseEmitter sseEmitter) {
        this(sseEmitter, m -> m);
    }

    public SSEEmitterStreamObserver(SseEmitter sseEmitter, Function<M, Object> mapping) {
        this.sseEmitter = sseEmitter;
        this.mapping = mapping;
    }

    @Override
    public void onNext(M message) {
        try {
            sseEmitter.send(mapping.apply(message));
        } catch (IOException e) {
            logger.debug("Error on sending {}", message);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        sseEmitter.completeWithError(throwable);
    }

    @Override
    public void onCompleted() {
        try {
            sseEmitter.send(SseEmitter.event().comment("End of stream"));
        } catch (IOException e) {
            logger.debug("Error on sending completed", e);
        }
        sseEmitter.complete();
    }

}

