package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class MetaDataBasedHandlerSelector implements HandlerSelector {

    private static final Logger logger = LoggerFactory.getLogger(MetaDataBasedHandlerSelector.class);

    private static Publisher<Score> selectHighestScore(Flux<Score> f) {
        AtomicInteger highestSoFarState = new AtomicInteger(Integer.MIN_VALUE);
        AtomicReference<Score> windowState = new AtomicReference<>();

        return f.filter(v -> {
                    int highestSoFar = highestSoFarState.get();
                    if (v.score > highestSoFar) {
                        highestSoFarState.set(v.score);
                        return true;
                    }
                    return v.score == highestSoFar;
                })
                .bufferUntil(i -> i != windowState.getAndSet(i), true)
                .flatMapIterable(Function.identity());
    }

    @Override
    public Flux<CommandHandlerSubscription> select(Flux<CommandHandlerSubscription> candidates, Command command) {
        return candidates
                .doFirst(() -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{}[{}] Selecting based on metadata", command.commandName(),
                                command.context());
                    }
                })
                .map(candidate -> scoreForCandidate(command, candidate))
                .flatMap(this::resolveScore)
                .transform(MetaDataBasedHandlerSelector::selectHighestScore)
                .map(score -> score.commandHandlerSubscription);
    }

    private ScoreMono scoreForCandidate(Command command, CommandHandlerSubscription c) {
        return new ScoreMono(c, calculateScore(command.metadata(), c.commandHandler()));
    }

    private Mono<Score> resolveScore(ScoreMono score) {
        return score
                .scoreMono
                .map(sc -> new Score(score.commandHandlerSubscription, sc));
    }

    private Mono<Integer> calculateScore(Metadata metaDataMap, CommandHandler client) {
        Metadata clientTags = client.metadata();
        return metaDataMap.metadataKeys()
                .filter(k -> !Metadata.isInternal(k))
                .reduce(0,
                        (score, key) -> score + match(metaDataMap.metadataValue(key),
                                clientTags.metadataValue(key)));
    }

    private int match(Optional<Serializable> requestValue, Optional<Serializable> handlerValue) {
        return !requestValue.isPresent() || !handlerValue.isPresent() ? 0 : matchValues(requestValue.get(),
                handlerValue.get());
    }

    private int matchValues(Serializable value, Serializable metaDataValue) {
        return String.valueOf(value).equals(String.valueOf(metaDataValue)) ? 1 : -1;
    }

    private static class Score {
        final CommandHandlerSubscription commandHandlerSubscription;
        final Integer score;


        private Score(CommandHandlerSubscription commandHandlerSubscription, Integer score) {
            this.commandHandlerSubscription = commandHandlerSubscription;
            this.score = score;
        }
    }

    private static class ScoreMono {
        final CommandHandlerSubscription commandHandlerSubscription;
        final Mono<Integer> scoreMono;

        private ScoreMono(CommandHandlerSubscription commandHandlerSubscription, Mono<Integer> scoreMono) {
            this.commandHandlerSubscription = commandHandlerSubscription;
            this.scoreMono = scoreMono;
        }
    }
}
