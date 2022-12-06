<h2>Components</h2>
```mermaid
classDiagram
    class EventStoreTransformationService {
        <<interface>>
        +start(context, description) transformationId
        +deleteEvent(context, transformationId, token, sequence)
        +replaceEvent(context, transformationId, token, event, sequence)
        +startCancelling(context, transformationId)
        +startApplying(context, transformationId, sequence)
        +compact(context)
        +transformations() transformations
        +transformationById() transformation
        +transformationByContext() transformation
    }
    LocalEventStoreTransformationService --|> EventStoreTransformationService: implements
    class Transformers {
        <<interface>>
        +transformerFor(context) ContextTransformer
    }
    LocalTransformers --|> Transformers: implements
    class ContextTransformer {
        <<interface>>
        +start(description) transformationId
        +deleteEvent(transformationId, token, sequence)
        +replaceEvent(transformationId, token, event, sequence)
        +startCancelling(transformationId)
        +markCancelled(transformationId)
        +startApplying(transformationId, sequence, applier)
        +markApplied(transformationId)
        +compact()
        +markCompacted()
    }
    SequentialContextTransformer --|> ContextTransformer: implements
    class EventStoreStateStore {
        <<interface>>
        +state(context) eventStoreState
        +save(eventStoreState)
    }
    JpaEventStoreStateStore --|> EventStoreStateStore
    class TransformationEntryStore {
        <<interface>>
        +store(transformationEntry) sequence
        +read() transformation entries
        +readFrom(sequence) transformationEntries
        +read(fromSequence, toSequence) transformationEntries
        +readClosed(fromSequence, toSequence) transformationEntries
        +readTo(sequence) transformationEntries
        +delete()
    }
    SegmentBasedTransformationEntryStore --|> TransformationEntryStore
    class ContextTransformationStore {
        <<interface>>
        +transformations() transformationStates
        +create(description) transformationState
        +transformation(id) transformationState
        +save(transformation state)
    }
    LocalContextTransformationStore --|> ContextTransformationStore
```