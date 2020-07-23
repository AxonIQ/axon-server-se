/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;

/**
 * @author Marc Gathier
 */
public class PersistedBloomFilter {
    private final Logger logger = LoggerFactory.getLogger(PersistedBloomFilter.class);

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private volatile BloomFilter<CharSequence> filter;
    private final Path path;
    private final int expectedInsertions;
    private final float fpp;

    public PersistedBloomFilter(String persistedPath, int expectedInsertions, float fpp) {
        path = Paths.get(persistedPath);

        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
    }

    public void load() {
        if (!fileExists()) throw new IllegalArgumentException("File does not exist");

        try (FileChannel channel = FileChannel.open(path)) {
            logger.debug("Opening bloom filter : {}", path);
            try( InputStream is = Channels.newInputStream(channel)) {
                filter = BloomFilter.readFrom(is, Funnels.stringFunnel(UTF8));
            }
        } catch (Exception ex) {
            throw new MessagingPlatformException(ErrorCode.INDEX_READ_ERROR,
                                                 "Error while opening bloom filter " + path,
                                                 ex);
        }


    }

    public void create() {
        filter = BloomFilter.create(Funnels.stringFunnel(UTF8),
                expectedInsertions,
                fpp);

    }

    public void store() {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            filter.writeTo(Channels.newOutputStream(channel));
        } catch (Exception ex) {
            logger.warn("Failed to store bloom filter {}", path, ex);
        }
    }

    public void insert(String key) {
        filter.put(key);
    }

    public boolean mightContain(String key) {
        return filter.mightContain(key);
    }

    public boolean fileExists() {
        return path.toFile().exists();
    }

    public void insertAll(Set<String> keys) {
        keys.forEach(key -> filter.put(key));
    }
}
