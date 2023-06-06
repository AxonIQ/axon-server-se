package io.axoniq.axonserver.eventstore.transformation.apply;

import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class CleanTransformationProgressStore implements CleanTransformationApplied {

    private final TransformationProgressStore transformationProgressStore;

    public CleanTransformationProgressStore(TransformationProgressStore transformationProgressStore) {
        this.transformationProgressStore = transformationProgressStore;
    }

    @Override
    public Mono<Void> clean() {
        return transformationProgressStore.clean();
    }
}
