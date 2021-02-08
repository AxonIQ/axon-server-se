/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

/**
 * Append-only list. List that only supports adding entries at the end, all other operations that attempt to
 * change the content of the list throw an {@link UnsupportedOperationException}.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@SuppressWarnings("squid:S2160")
public class AppendOnlyList<T> extends AbstractList<T> {

    public static final String LIST_IS_APPEND_ONLY = "List is append only";
    private final Node<T> head;
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicReference<Node<T>> last = new AtomicReference<>();

    public AppendOnlyList(List<T> values) {
        head = new Node<>(values);
        last.set(head);
        size.set(values.size());
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    public void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    protected void removeRange(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    public void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
    }

    public T last() {
        List<? extends T> lastValues = last.get().values;
        return lastValues.get(lastValues.size() - 1);
    }

    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Nonnull
    @Override
    public Iterator<T> iterator() {
        int sizeAtStart = size.get();
        return new Iterator<T>() {
            Node<T> currentNode = head;
            int index = 0;
            int count = 0;

            @Override
            public boolean hasNext() {
                return count < sizeAtStart;
            }

            @Override
            public T next() {
                T value = null;
                if (currentNode.size() > index) {
                    value = currentNode.get(index);
                    index++;
                    count++;
                } else {
                    currentNode = currentNode.next;
                    index = 0;
                    value = next();
                }
                return value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException(LIST_IS_APPEND_ONLY);
            }
        };
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public boolean addAll(Collection<? extends T> values) {
        last.updateAndGet(l -> l.add(values));
        size.addAndGet(values.size());
        return true;
    }

    @Override
    public boolean add(T value) {
        return addAll(Collections.singletonList(value));
    }

    @Override
    public T get(int index) {
        int pos = 0;
        if (index < 0 || index >= size.get()) {
            throw new IndexOutOfBoundsException(String.format("%d: index out of bounds [0..%d]",
                                                              index,
                                                              size.get() - 1));
        }
        Node<T> node = head;
        while (pos + node.size() <= index) {
            pos += node.size();
            node = node.next;
        }
        return node.get(index - pos);
    }

    private static class Node<T> {

        private final List<? extends T> values;
        private Node<T> next;

        private Node(List<? extends T> values) {
            this.values = values;
        }

        private Node<T> add(Collection<? extends T> values) {
            this.next = new Node<>(asList(values));
            return this.next;
        }

        private List<? extends T> asList(Collection<? extends T> values) {
            if (values instanceof List) {
                return (List<? extends T>) values;
            }
            return new ArrayList<>(values);
        }

        public int size() {
            return values.size();
        }

        public T get(int index) {
            return values.get(index);
        }
    }
}
