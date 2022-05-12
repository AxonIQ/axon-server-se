package io.axoniq.axonserver.localstorage.transformation;

interface TransformationApplyingState {

    long lastAppliedSequence();

    boolean applied();
}
