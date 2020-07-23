package io.axoniq.axonserver.enterprise.storage.file.xref;

import io.axoniq.axonserver.localstorage.file.FileUtils;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Manages a global index containing the last token and sequence number per aggregate.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class GlobalIndex {

    private static final String AGGREGATE_MAP = "aggregateMap";

    private static final Serializer<LastEventPositionInfo> GLOBAL_INDEX_SERIALIZER = new Serializer<LastEventPositionInfo>() {
        public void serialize(@Nonnull DataOutput2 dataOutput2, @Nonnull LastEventPositionInfo positionInfo) {
            dataOutput2.packLong(positionInfo.token());
            dataOutput2.packLong(positionInfo.lastSequence());
        }

        public LastEventPositionInfo deserialize(@Nonnull DataInput2 dataInput2, int available) throws IOException {
            return new LastEventPositionInfo(dataInput2.unpackLong(), dataInput2.unpackLong());
        }
    };

    private final File directory;
    private final int maxEntries;
    private final String fileFormat;
    private final Pattern pattern;

    private final AtomicReference<GlobalIndexSegment> last = new AtomicReference<>();

    /**
     * Creates a GlobalIndex with approximately 1 million of keys per segment
     *
     * @param storageProperties configuration of the event store
     * @param context           the context name
     */
    public GlobalIndex(StorageProperties storageProperties, String context) {
        this(storageProperties, context, 1_000_000);
    }

    /**
     * Creates a GlobalIndex with approximately {@code maxEntries} of keys per segment
     *
     * @param storageProperties configuration of the event store
     * @param context           the context name
     * @param maxEntries        number of keys per segment
     */
    public GlobalIndex(StorageProperties storageProperties, String context, int maxEntries) {
        this.fileFormat = "global-index-%05d" + storageProperties.getGlobalIndexSuffix();
        this.pattern = Pattern.compile("global-index-(\\d+)" + storageProperties.getGlobalIndexSuffix());
        this.directory = new File(storageProperties.getStorage(context));
        this.maxEntries = maxEntries;
    }

    /**
     * Initializes the global index, finding the segments.
     */
    public void init() {
        File[] files = directory.listFiles((dir, name) -> isGlobalIndexFile(name));
        if (files == null) {
            throw new RuntimeException("Could not list files in " + directory.getAbsolutePath());
        }

        for (File file : files) {
            last.set(new GlobalIndexSegment(file, last.get()));
        }
    }

    /**
     * Adds all entries to the global index. If the number of entries in the current segment exceeds the maximum
     * number of entries defined it will start a new segment and try to compress older segments.
     *
     * @param entries the entries to add
     */
    public void add(Map<String, JumpSkipIndexEntries> entries) {
        GlobalIndexSegment lastSegment = last.get();
        if (lastSegment == null) {
            lastSegment = new GlobalIndexSegment(createFile(0), null);
            last.set(lastSegment);
        }
        if (lastSegment.size() > maxEntries) {
            compress();
            File nextSegment = createFile(lastSegment.index() + 1);
            last.set(new GlobalIndexSegment(nextSegment, lastSegment));
        }

        lastSegment.add(entries);
    }

    /**
     * Returns the sequence number and token of the last event for the id. If not found it returns null.
     *
     * @param id the id of the aggregate
     * @return sequence number and token of the last event for the id
     */
    public LastEventPositionInfo get(String id) {
        return getOrDefault(id, null);
    }

    /**
     * Returns the sequence number and token of the last event for the id. If not found it returns {@code defaultValue}.
     *
     * @param id           the id of the aggregate
     * @param defaultValue the value to return when not found
     * @return sequence number and token of the last event for the id
     */
    public LastEventPositionInfo getOrDefault(String id, LastEventPositionInfo defaultValue) {
        return last.get() == null ? defaultValue : last.get().getOrDefault(id, defaultValue);
    }

    /**
     * Tries to merge older segments containing a limited number of keys.
     */
    public void compress() {
        last.get().compress(2);
    }

    /**
     * Closes the global index (and all its segments).
     */
    public void close() {
        if (last.get() != null) {
            last.get().closeAll();
        }
    }

    /**
     * Deletes the global index (and all its segments).
     */
    public void delete() {
        if (last.get() != null) {
            last.get().deleteAll();
        }
    }

    private File createFile(int i) {
        return new File(directory.getAbsolutePath() + File.separatorChar + String.format(fileFormat, i));
    }

    private boolean isGlobalIndexFile(String name) {
        return pattern.matcher(name).matches();
    }

    public Stream<String> files() {
        List<String> segments = new LinkedList<>();
        GlobalIndexSegment segment = last.get();
        while (segment != null) {
            segments.add(0, segment.file.getAbsolutePath());
            segment = segment.previousSegment;
        }
        return segments.stream();
    }

    private class GlobalIndexSegment {

        private final File file;
        private final DB db;
        private volatile GlobalIndexSegment previousSegment;
        private final HTreeMap<String, LastEventPositionInfo> map;

        public GlobalIndexSegment(File file, GlobalIndexSegment previousSegment) {
            this.file = file;
            deleteWriteAheadLogs();
            this.db = DBMaker.fileDB(file)
                             .fileMmapEnable()
                             .cleanerHackEnable()
                             .transactionEnable()
                             .allocateIncrement(128L * maxEntries)
                             .make();
            this.previousSegment = previousSegment;

            this.map = db.hashMap(AGGREGATE_MAP,
                                  Serializer.STRING,
                                  GLOBAL_INDEX_SERIALIZER)
                         .counterEnable()
                         .createOrOpen();
            this.db.commit();
        }

        private void deleteWriteAheadLogs() {
            File[] walFiles = file.getParentFile().listFiles(n -> n.getName().startsWith(file.getName() + ".wal"));
            for (File walFile : walFiles) {
                FileUtils.delete(walFile);
            }
        }

        private void compress(int skip) {
            if (previousSegment != null) {
                previousSegment.compress(skip - 1);
                if (skip <= 0 && size() + previousSegment.size() < maxEntries) {
                    GlobalIndexSegment old = previousSegment;
                    old.map.forEach(map::putIfAbsent);
                    db.commit();
                    this.previousSegment = old.previousSegment;
                    old.remove();
                }
            }
        }

        private void deleteAll() {
            remove();
            if (previousSegment != null) {
                previousSegment.deleteAll();
            }
        }

        private void remove() {
            db.close();
            FileUtils.delete(file);
        }

        private void closeAll() {
            db.close();
            if (previousSegment != null) {
                previousSegment.closeAll();
            }
        }

        private void add(Map<String, JumpSkipIndexEntries> entries) {
            Set<String> deleteFromPrevious = new HashSet<>();
            entries.forEach((key, value) -> {
                LastEventPositionInfo old = map.put(key, value.lastEventPositionInfo());
                if (old == null && value.previousToken() >= 0) {
                    deleteFromPrevious.add(key);
                }
            });
            db.commit();
            if (previousSegment != null) {
                previousSegment.deleteKeys(deleteFromPrevious);
            }
        }

        private void deleteKeys(Set<String> keys) {
            if (keys.isEmpty()) {
                return;
            }
            Set<String> deleteFromPrevious = new HashSet<>();
            keys.forEach(s -> {
                if (map.remove(s) == null) {
                    deleteFromPrevious.add(s);
                }
            });
            db.commit();
            if (previousSegment != null) {
                previousSegment.deleteKeys(deleteFromPrevious);
            }
        }

        private int size() {
            return map.size();
        }

        private LastEventPositionInfo getOrDefault(String id, LastEventPositionInfo defaultValue) {
            LastEventPositionInfo value = map.get(id);
            if (value != null) {
                return value;
            }
            return previousSegment != null ? previousSegment.getOrDefault(id, defaultValue) : defaultValue;
        }

        private int index() {
            Matcher m = pattern.matcher(file.getName());
            if (!m.matches()) {
                throw new RuntimeException("Illegal global index segment file name");
            }
            return Integer.parseInt(m.group(1));
        }
    }
}
