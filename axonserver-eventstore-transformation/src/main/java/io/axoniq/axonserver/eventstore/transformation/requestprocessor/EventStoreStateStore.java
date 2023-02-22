package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

/**
 * Stores the state of the event store in regard to the event transformation.
 * For each context, the store can provide the current state and the identifier of the operation that is in progress.
 * The state could be:
 * IDLE, if no transformation is in progress;
 * TRANSFORMING, if there is an event trasformation in progress;
 * COMPACTIONG, when the event store compaction is in progress.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface EventStoreStateStore {

    /**
     * Returns the current state for the event store in regard to the event transformation.
     *
     * @param context the context to be retrieved
     * @return the current state for the event store in regard to the event transformation.
     */
    EventStoreState state(String context);

    /**
     * Saves the specified state in the store.
     *
     * @param state the state to be saved into the store.
     */
    void save(EventStoreState state);


    void clean(String context);

    /**
     * The state a specific context could be in regard to event transformation.
     */
    enum State {
        /**
         * When there are no operation in progress in regard to event transformation.
         */
        IDLE,
        /**
         * When the event store compaction is in progress (the obsoleted events versions are going to be deleted)
         */
        COMPACTING,
        /**
         * When a transformation is in progress.
         */
        TRANSFORMING
    }

    /**
     * Visits the {@link EventStoreState} to collect the state.
     */
    interface Visitor {

        /**
         * Collects the context of the visited {@link EventStoreState} and return itself for fluid api.
         *
         * @param context the context of the visited {@link EventStoreState}
         * @return the visitor for fluid api
         */
        Visitor setContext(String context);

        /**
         * Collects the state of the visited {@link EventStoreState} and return itself for fluid api.
         *
         * @param state the state of the visited {@link EventStoreState}
         * @return the visitor for fluid api
         */
        Visitor setState(State state);

        /**
         * Collects the in-progress operation id of the visited {@link EventStoreState} and return itself for fluid api.
         *
         * @param operationId the id of the operation that is progress for the visited {@link EventStoreState}
         * @return the visitors for fluid api
         */
        Visitor setOperationId(String operationId);
    }

    /**
     * The state of an event store for a specific context.
     */
    interface EventStoreState {

        /**
         * Accept a visitor of the state.
         *
         * @param visitor the visitor
         */
        void accept(Visitor visitor);

        /**
         * Validates if it is possible to pass from the current state to the transforming one.
         * Returns a new instance of the {@link EventStoreState} that represent the new transforming state.
         *
         * @param transformationId the identifier of the transformation
         * @return a new instance of the {@link EventStoreState} that represent the new transforming state.
         */
        default EventStoreState transform(String transformationId) {
            throw new WrongTransformationStateException("Unsupported operation");
        }

        /**
         * Validates if it is possible to pass from the current state to the cancelled one.
         * Returns a new instance of the {@link EventStoreState} that represent the new cancelled state.
         *
         * @return a new instance of the {@link EventStoreState} that represent the new cancelled state.
         */
        default EventStoreState cancelled() {
            throw new WrongTransformationStateException("Unsupported operation");
        }

        /**
         * Validates if it is possible to pass from the current state to the transformed one.
         * Returns a new instance of the {@link EventStoreState} that represent the new transformed state.
         *
         * @return a new instance of the {@link EventStoreState} that represent the new transformed state.
         */
        default EventStoreState transformed() {
            throw new WrongTransformationStateException("Unsupported operation");
        }

        /**
         * Validates if it is possible to pass from the current state to the compacting one.
         * Returns a new instance of the {@link EventStoreState} that represent the new compacting state.
         *
         * @param compactionId the identifier of the compaction operation
         * @return a new instance of the {@link EventStoreState} that represent the new compacting state.
         */
        default EventStoreState compact(String compactionId) {
            throw new WrongTransformationStateException("Unsupported operation");
        }

        /**
         * Validates if it is possible to pass from the current state to the compacted one.
         * Returns a new instance of the {@link EventStoreState} that represent the new compacted state.
         *
         * @return a new instance of the {@link EventStoreState} that represent the new compacted state.
         */
        default EventStoreState compacted() {
            throw new WrongTransformationStateException("Unsupported operation");
        }
    }
}
