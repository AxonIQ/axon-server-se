package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public class PrimaryEventStoreFactory {
    public static PrimaryEventStore create(String path) {
        String context= "default";
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        StorageProperties storageOptions = new StorageProperties();
        storageOptions.setSegmentSize(1024*1024);
        storageOptions.setLogStorageFolder(path);


        IndexManager indexManager = new IndexManager(storageOptions, context);
        PrimaryEventStore primary = new PrimaryEventStore(context,
                                        indexManager,
                                        eventTransformerFactory,
                                        storageOptions);
        primary.next = new SecondaryEventStore(context, indexManager, eventTransformerFactory, storageOptions);
        primary.initSegments(Long.MAX_VALUE);
        return primary;
    }

}
