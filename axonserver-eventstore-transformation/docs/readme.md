
## Event Transformation SQL TABLES in controlDB

### ET_EVENT_STORE_STATE

For each context, it contains the state of the event store, accordingly to [the state diagram](state.md)

### ET_EVENT_STORE_TRANSFORMATION

It contains the list of all event transformations' current state.

### ET_LOCAL_EVENT_STORE_TRANSFORMATION

It contains the progress of the apply task on the local event store.

### ET_LOCAL_CLEANED_EVENT_STORE_TRANSFORMATION

It contains transformations that have already been cleaned locally.