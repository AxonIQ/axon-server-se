package io.axoniq.axonserver.eventstore.transformation.apply;

public interface TransformationApplyingState {

    long lastAppliedSequence();

    boolean applied();
}
