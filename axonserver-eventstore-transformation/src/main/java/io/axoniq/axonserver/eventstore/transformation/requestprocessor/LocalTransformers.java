package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.filestorage.AppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.BaseAppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.impl.StorageProperties;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class LocalTransformers implements Transformers {

    private final Map<String, ContextTransformer> transformers = new ConcurrentHashMap<>();
    private final EventStoreTransformationRepository transformationRepository;
    private final Function<String, EventProvider> eventProviderFactory;
    private final Function<String, TransformationApplyTask> applyTaskFactory;
    private final Function<String, TransformationRollBackTask> rollbackTaskFactory;


    public LocalTransformers(Function<String, EventProvider> eventProviderFactory,
                             EventStoreTransformationRepository eventStoreTransformationRepository) {
        this.eventProviderFactory = eventProviderFactory;
        this.transformationRepository = eventStoreTransformationRepository;
    }

    @Override
    public ContextTransformer transformerFor(String context) {
        return transformers.computeIfAbsent(context, c -> {
            String baseDirectory = "/home/milansavic/" + context; //TODO!!!
            StorageProperties storageProperties = new StorageProperties();
            storageProperties.setStorage(Paths.get(baseDirectory, "transformation").toFile());
            storageProperties.setSuffix(".actions");
            AppendOnlyFileStore fileStore = new BaseAppendOnlyFileStore(storageProperties, context);
            fileStore.open(false).block();// TODO: 3/18/22 block???
            TransformationEntryStore transformationEntryStore = new SegmentBasedTransformationEntryStore(fileStore);
            ContextTransformationStore store = new LocalContextTransformationStore(context, transformationRepository,
                                                                                   transformationEntryStore);
            EventProvider eventProvider = eventProviderFactory.apply(context);
            TransformationStateConverter converter = new ContextTransformationStateConverter(eventProvider);
            return new SequentialContextTransformer(context, store, converter);
        });
    }
}
