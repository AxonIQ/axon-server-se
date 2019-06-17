package io.axoniq.axonserver.cluster.replication.file;

/**
 * @author Marc Gathier
 */
public class PrimaryEventStoreFactory {
    public static PrimaryLogEntryStore create(String path) {
        return create(path, "default");
    }

    public static PrimaryLogEntryStore create(String path, String context) {
        LogEntryTransformerFactory eventTransformerFactory = new DefaultLogEntryTransformerFactory();
        StorageProperties storageOptions = new StorageProperties();
        storageOptions.setSegmentSize(1024*1024);
        storageOptions.setLogStorageFolder(path);


        IndexManager indexManager = new IndexManager(storageOptions, context);
        PrimaryLogEntryStore primary = new PrimaryLogEntryStore(context,
                                                                indexManager,
                                                                eventTransformerFactory,
                                                                storageOptions);
        primary.next = new SecondaryLogEntryStore(context, indexManager, eventTransformerFactory, storageOptions);
        primary.initSegments(Long.MAX_VALUE);
        return primary;
    }

}
