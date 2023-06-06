package io.axoniq.axonserver.eventstore.transformation.apply;

import io.axoniq.axonserver.eventstore.transformation.ActionSupplier;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class TransformationApplyAction implements ActionSupplier {

    private static final Logger logger = LoggerFactory.getLogger(TransformationApplyAction.class);

    private final TransformationApplyExecutor applier;
    private final MarkTransformationApplied markTransformationApplied;

    private final CleanTransformationApplied cleanTransformationApplied;
    private final Transformations transformations;

    public TransformationApplyAction(TransformationApplyExecutor applier,
                                     MarkTransformationApplied markTransformationApplied,
                                     CleanTransformationApplied cleanTransformationApplied,
                                     Transformations transformations) {
        this.applier = applier;
        this.markTransformationApplied = markTransformationApplied;
        this.cleanTransformationApplied = cleanTransformationApplied;
        this.transformations = transformations;
    }


    @Override
    public Mono<Void> get() {
        return transformations.applyingTransformations()
                              .doOnNext(transformation -> logger.info("Applying transformation: {}",
                                                                      transformation.id()))
                              .flatMap(transformation -> applier.apply(new ApplierTransformation(transformation))
                                                                .doOnError(t -> logger.error(
                                                                        "An error happened while applying the transformation: {}.",
                                                                        transformation.id(),
                                                                        t))
                                                                .doOnSuccess(notUsed -> logger.info(
                                                                        "Applied transformation: {}",
                                                                        transformation.id()))
                                                                .then(markTransformationApplied.markApplied(
                                                                        transformation.context(),
                                                                        transformation.id())))
                              .then(cleanTransformationApplied.clean());
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
