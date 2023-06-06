/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

/**
 * @author Stefan Dragisic
 */

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface Segment {

    Supplier<InputStream> contentProvider();

    FileVersion id();

    Supplier<List<File>> previousVersions();

    Supplier<List<File>> indexProvider();

    Stream<AggregateSequence> latestSequenceNumbers();
}
