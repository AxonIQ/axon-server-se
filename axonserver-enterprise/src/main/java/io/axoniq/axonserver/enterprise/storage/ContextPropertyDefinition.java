package io.axoniq.axonserver.enterprise.storage;

import io.axoniq.axonserver.enterprise.storage.file.EmbeddedDBPropertiesProvider;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.core.convert.ConversionService;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * @author Marc Gathier
 */
public enum ContextPropertyDefinition implements Printable {

    EVENT_STORAGE_DIRECTORY(EventType.EVENT, "event.storage",
                            "Location where the event store's event information will be stored",
                            Type.STRING,
                            StorageProperties::withStorage),
    EVENT_SEGMENT_SIZE(EventType.EVENT,
                       "event.segment-size", "Size of the event store segments (in bytes)",
                       Type.LONG,
                       (old, v) -> old.withSegmentSize(Long.parseLong(v))),
    EVENT_MAX_BLOOM_FILTERS_IN_MEMORY(EventType.EVENT,
                                      "event.max-bloom-filters-in-memory",
                                      "Number of bloom filters for events to be kept in memory",
                                      Type.INT,
                                      (old, v) -> old.withMaxBloomFiltersInMemory(Integer.parseInt(v))
    ),
    EVENT_MAX_INDEXES_IN_MEMORY(EventType.EVENT, "event.max-indexes-in-memory",
                                "Number of index files for events to be kept in memory",
                                Type.INT,
                                (old, v) -> old.withMaxIndexesInMemory(Integer.parseInt(v))),
    EVENT_INDEX_FORMAT(EventType.EVENT,
                       "event.index-format",
                       "Index type used for the events this context",
                       Type.STRING,
                       StorageProperties::withIndexFormat,
                       EmbeddedDBPropertiesProvider.JUMP_SKIP_INDEX,
                       EmbeddedDBPropertiesProvider.BLOOM_FILTER_INDEX),
    EVENT_RETENTION_TIME(EventType.EVENT,
                         "event.retention-time",
                         "Retention time for the events in the primary locations (if secondary location specified)",
                         Type.DURATION,
                         (old, v) -> old.withRetentionTime(parseDurations(v))),
    SNAPSHOT_STORAGE_DIRECTORY(EventType.SNAPSHOT,
                               "snapshot.storage",
                               "Location where the event store's snapshot information will be stored",
                               Type.STRING,
                               StorageProperties::withStorage),
    SNAPSHOT_SEGMENT_SIZE(EventType.SNAPSHOT,
                          "snapshot.segment-size",
                          "Size of the event store snapshot segments (in bytes)",
                          Type.LONG,
                          (old, v) -> old.withSegmentSize(Long.parseLong(v))),
    SNAPSHOT_MAX_BLOOM_FILTERS_IN_MEMORY(EventType.SNAPSHOT,
                                         "snapshot.max-bloom-filters-in-memory",
                                         "Number of bloom filters for snapshots to be kept in memory",
                                         Type.INT,
                                         (old, v) -> old.withMaxBloomFiltersInMemory(Integer.parseInt(v))),
    SNAPSHOT_MAX_INDEXES_IN_MEMORY(EventType.SNAPSHOT,
                                   "snapshot.max-indexes-in-memory",
                                   "Number of index files for snapshots to be kept in memory",
                                   Type.INT,
                                   (old, v) -> old.withMaxIndexesInMemory(Integer.parseInt(v))),
    SNAPSHOT_INDEX_FORMAT(EventType.SNAPSHOT,
                          "snapshot.index-format",
                          "Index type used for the snapshot in this context",
                          Type.STRING,
                          StorageProperties::withIndexFormat,
                          EmbeddedDBPropertiesProvider.JUMP_SKIP_INDEX,
                          EmbeddedDBPropertiesProvider.BLOOM_FILTER_INDEX),
    SNAPSHOT_RETENTION_TIME(EventType.SNAPSHOT,
                            "snapshot.retention-time",
                            "Retention time for the events in the primary locations (if secondary location specified)",
                            Type.DURATION,
                            (old, v) -> old.withRetentionTime(parseDurations(v))),
    ;

    private final EventType eventType;
    private final String key;
    private final String description;
    private final Type type;
    private final BiFunction<StorageProperties, String, StorageProperties> updateFunction;
    private final String[] options;

    public EventType scope() {
        return eventType;
    }

    public StorageProperties apply(StorageProperties storageProperties, String value) {
        try {
            return updateFunction.apply(storageProperties, value);
        } catch (Exception ex) {
            return storageProperties;
        }
    }

    public String key() {
        return key;
    }

    public enum Type {
        STRING(s -> true),
        LONG(Type::checkLong),
        INT(Type::checkInt),
        DURATION(Type::checkDuration);

        static boolean checkLong(String value) {
            try {
                Long.parseLong(value);
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        static boolean checkInt(String value) {
            try {
                Integer.parseInt(value);
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        static boolean checkDuration(String value) {
            try {
                parseDurations(value);
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        private final Predicate<String> validator;

        Type(Predicate<String> validator) {
            this.validator = validator;
        }

        boolean validate(String value) {
            return validator.test(value);
        }
    }

    ContextPropertyDefinition(EventType eventType, String key, String description, Type type,
                              BiFunction<StorageProperties, String, StorageProperties> updateFunction) {
        this(eventType, key, description, type, updateFunction, new String[0]);
    }

    ContextPropertyDefinition(EventType eventType, String key, String description, Type type,
                              BiFunction<StorageProperties, String, StorageProperties> updateFunction,
                              String... options) {
        this.eventType = eventType;
        this.key = key;
        this.description = description;
        this.type = type;
        this.updateFunction = updateFunction;
        this.options = options;
    }

    public void validate(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "%s: value cannot be empty",
                    key));
        }

        if (options.length > 0 && !Arrays.asList(options).contains(value)) {
            throw new IllegalArgumentException(String.format("%s: value must be one of %s",
                                                             key,
                                                             String.join(",", options)));
        }

        if (!type.validate(value)) {
            throw new IllegalArgumentException(String.format("%s: invalid value %s", key, value));
        }
    }

    public static ContextPropertyDefinition findByKey(String key) {
        for (ContextPropertyDefinition value : values()) {
            if (value.key.equals(key)) {
                return value;
            }
        }
        return null;
    }

    private static Duration[] parseDurations(String value) {
        ConversionService conversionService = ApplicationConversionService
                .getSharedInstance();
        return conversionService.convert(value, Duration[].class);
    }

    @Override
    public void printOn(Media media) {
        media.with("key", key)
             .with("description", description)
             .with("type", type.name())
             .with("scope", eventType.name())
             .withStrings("options", Arrays.asList(options));
    }
}
