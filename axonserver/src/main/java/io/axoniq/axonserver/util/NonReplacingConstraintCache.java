package io.axoniq.axonserver.util;

/**
 * A specialized ConstraintCache that provides ways to prevent inserting values for already existing keys
 *
 * @author Marco Amann
 * @since 4.6
 */
public interface NonReplacingConstraintCache<K, V> extends ConstraintCache<K, V> {

    /**
     * Inserts a new value, if the key is not present yet. In line with the requirements of this method in
     * java.util.Map, any implementation providing atomicity guarantees must override this method and document its
     * concurrency properties.
     *
     * @param key   the identifier of the item
     * @param value the item to be cached
     * @return the previus value if present, null otherwise
     */
    V putIfAbsent(K key, V value);

    @Override
    @Deprecated
    default V put(K key, V value) {
        throw new UnsupportedOperationException();
    }
}
