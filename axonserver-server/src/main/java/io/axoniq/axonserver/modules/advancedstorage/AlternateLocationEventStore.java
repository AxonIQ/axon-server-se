package io.axoniq.axonserver.modules.advancedstorage;

import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.SecondaryEventStore;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.IOException;

/**
 * Author: marc
 */
public class AlternateLocationEventStore extends SecondaryEventStore {

    private final StorageProperties primaryStorageProperties;

    public AlternateLocationEventStore(EventTypeContext eventTypeContext, IndexManager altIndexManager,
                                       EventTransformerFactory eventTransformerFactory,
                                       StorageProperties primaryStorageProperties,
                                       StorageProperties eventSecondary) {
        super(eventTypeContext, altIndexManager, eventTransformerFactory, eventSecondary);
        this.primaryStorageProperties = primaryStorageProperties;
    }

    @Override
    public void handover(Long segment, Runnable callback) {
        if( safeCopy(primaryStorageProperties.bloomFilter(context, segment), storageProperties.bloomFilter(context, segment))
                && safeCopy(primaryStorageProperties.index(context, segment), storageProperties.index(context, segment))
                && safeCopy(primaryStorageProperties.dataFile(context, segment), storageProperties.dataFile(context, segment))
        ) {
            callback.run();
        }

    }

    private boolean safeCopy( File src, File dest) {
        if (!src.exists()) return true;
        if (dest.exists()) return true;

        logger.warn("Copy {} to {}", src, dest);
        File destTemp = new File(dest.getAbsolutePath() + ".tmp");
        try {
            FileCopyUtils.copy(src, destTemp);
        } catch (IOException e) {
            logger.info("Failed to copy file {} to {}", src, dest, e);
            return false;
        }
        return destTemp.renameTo(dest);
    }
}
