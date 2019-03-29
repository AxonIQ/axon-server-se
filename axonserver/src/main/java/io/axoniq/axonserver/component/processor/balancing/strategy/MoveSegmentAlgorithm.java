/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.LoadBalancingOperation;
import io.axoniq.axonserver.component.processor.balancing.operation.OperationSequence;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Sara Pellegrini on 09/08/2018.
 * sara.pellegrini@gmail.com
 */
class MoveSegmentAlgorithm<T extends MoveSegmentAlgorithm.Instance<T>> {

    public interface Instance<T extends Instance> extends Comparable<T> {

        boolean canAcceptThreadFrom(T source);

        Integer acceptThreadFrom(T source);

    }

    public interface MoveOperationFactory<T extends Instance> {

        LoadBalancingOperation apply(Integer segmentId, T source, T target);

    }

    private final MoveOperationFactory<T> move;

    MoveSegmentAlgorithm(MoveOperationFactory<T> move) {
        this.move = move;
    }

    public LoadBalancingOperation balance(Iterable<T> projections){
        List<LoadBalancingOperation> ops = new LinkedList<>();
        LinkedList<T> instances = new LinkedList<>();
        projections.forEach(instances::add);
        LoadBalancingOperation operation;
        do {
            operation = findOperation(instances);
            if (operation != null) {
                ops.add(operation);
            }
        } while (operation!= null);
        return new OperationSequence(ops);
    }

    private LoadBalancingOperation findOperation(LinkedList<T> instances) {
        instances.sort(Comparator.naturalOrder());
        for (int i = instances.size() - 1; i > 0 ; i--) {
             T source = instances.get(i);
            for (int j = 0 ; j < i; j++) {
                T target = instances.get(j);
                if (target.canAcceptThreadFrom(source)) {
                    Integer segmentId = target.acceptThreadFrom(source);
                    return move.apply(segmentId, source, target);
                }
            }
        }
        return null;
    }
}
