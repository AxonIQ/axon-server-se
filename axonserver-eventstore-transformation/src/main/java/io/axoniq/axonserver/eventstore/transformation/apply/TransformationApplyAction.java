package io.axoniq.axonserver.eventstore.transformation.apply;

import io.axoniq.axonserver.eventstore.transformation.ActionSupplier;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import io.axoniq.axonserver.util.IdLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class TransformationApplyAction implements ActionSupplier {

    private static final Logger logger = LoggerFactory.getLogger(TransformationApplyAction.class);

    private static final String LOCK_ID_FORMAT = "Event-Transformation-%s";

    private final TransformationApplyExecutor applier;
    private final MarkTransformationApplied markTransformationApplied;

    private final CleanTransformationApplied cleanTransformationApplied;
    private final Transformations transformations;

    private final Function<String, IdLock> eventStoreLockPerContextProvider;

    public TransformationApplyAction(TransformationApplyExecutor applier,
                                     MarkTransformationApplied markTransformationApplied,
                                     CleanTransformationApplied cleanTransformationApplied,
                                     Transformations transformations,
                                     Function<String, IdLock> eventStoreLockPerContextProvider) {
        this.applier = applier;
        this.markTransformationApplied = markTransformationApplied;
        this.cleanTransformationApplied = cleanTransformationApplied;
        this.transformations = transformations;
        this.eventStoreLockPerContextProvider = eventStoreLockPerContextProvider;
    }


    @Override
    public Mono<Void> get() {
        return transformations.applyingTransformations()
                              .doOnNext(transformation -> logger.info("Applying transformation: {}",
                                                                      transformation.id()))
                              .flatMap(this::apply)
                              .then(cleanTransformationApplied.clean());
    }

    private Mono<Void> apply(Transformation transformation) {
        return Mono.defer(() -> {
            IdLock.Ticket ticket = eventStoreLockPerContextProvider
                    .apply(transformation.context())
                    .request(String.format(LOCK_ID_FORMAT, transformation.id()));
            if (!ticket.isAcquired()) {
                return Mono.empty();
            }
            return applier.apply(new ApplierTransformation(transformation))
                          .doOnError(t -> logger.error(
                                  "An error happened while applying the transformation: {}.",
                                  transformation.id(),
                                  t))
                          .doOnSuccess(notUsed -> logger.info(
                                  "Applied transformation: {}",
                                  transformation.id()))
                          .then(markTransformationApplied.markApplied(
                                  transformation.context(),
                                  transformation.id()))
                          .doFinally(s -> ticket.release());
        });
    }


    static class ApplierTransformation implements TransformationApplyExecutor.Transformation {

        private final Transformation state;

        ApplierTransformation(Transformation state) {
            this.state = state;
        }

        @Override
        public String id() {
            return state.id();
        }

        @Override
        public String context() {
            return state.context();
        }

        @Override
        public int version() {
            return state.version();
        }

        @Override
        public long lastSequence() {
            return state.lastSequence()
                        .orElseThrow(() -> new IllegalStateException("The last sequence of transformation is " + id()));
        }

        @Override
        public String toString() {
            return "ApplierTransformation{" +
                    "state=" + state +
                    ", context='" + context() + '\'' +
                    '}';
        }
    }
}
