/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.test.FakeClock;
import org.junit.*;

import java.util.Optional;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SequenceNumberCacheTest {

    private FakeClock clock = new FakeClock();
    private SequenceNumberCache testSubject = new SequenceNumberCache(SequenceNumberCacheTest::dummySequenceNumberProvider,
                                                                      clock, 1);

    private static Optional<Long> dummySequenceNumberProvider(String aggregateId,
                                                              EventStorageEngine.SearchHint... searchHints) {
        switch (aggregateId) {
            case "NEW":
                return Optional.empty();
            default:
                return Optional.ofNullable(10L);
        }
    }

    private static Optional<Long> slowSequenceNumberProvider(String aggregateId,
                                                             EventStorageEngine.SearchHint... searchHints) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Test
    public void testParallelTransactions() {
        testSubject = new SequenceNumberCache(SequenceNumberCacheTest::slowSequenceNumberProvider,
                                              clock, 1);

        boolean[] success = new boolean[2];
        IntStream.range(0, 2).parallel().forEach(i -> {
            try {
                testSubject.reserveSequenceNumbers(asList(
                        serializedEvent("NEW", "SampleAgg", 0)));
                success[i] = true;
            } catch (MessagingPlatformException ex) {
                Assert.assertEquals(ErrorCode.INVALID_SEQUENCE, ex.getErrorCode());
                success[i] = false;
            }
        });

        assertTrue("Expect one success", success[0] || success[1]);
        assertFalse("Expect one failure", success[0] && success[1]);

        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("NEW", "SampleAgg", 1)));
    }

    @Test
    public void reserveSequenceNumbers() {
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("NEW", "SampleAgg", 0)));
    }

    @Test
    public void reserveSequenceNumbersInvalidSequence() {
        try {
            testSubject.reserveSequenceNumbers(asList(
                    serializedEvent("OTHER", "SampleAgg", 0)));
            fail("Should fail, as sequence number should be 11");
        } catch (MessagingPlatformException mpe) {
            assertEquals(ErrorCode.INVALID_SEQUENCE, mpe.getErrorCode());
        }
    }
    @Test
    public void reserveSequenceNumbersValidAfterInvalidSequence() {
        try {
            testSubject.reserveSequenceNumbers(asList(
                    serializedEvent("OTHER", "SampleAgg", 12)));
            fail("Should fail, as sequence number should be 11");
        } catch (MessagingPlatformException mpe) {
            assertEquals(ErrorCode.INVALID_SEQUENCE, mpe.getErrorCode());
        }
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("OTHER", "SampleAgg", 11)));
    }

    @Test
    public void reserveSequenceNumbersValidSequence() {
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("OTHER", "SampleAgg", 11)));
    }

    @Test
    public void reserveSequenceNumbersValidSequenceMultipleEvents() {
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("OTHER", "SampleAgg", 11),
                serializedEvent("OTHER", "SampleAgg", 12)
        ));
    }
    @Test
    public void reserveSequenceNumbersValidSequenceMultipleEvents2() {
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("OTHER", "SampleAgg", 11)
        ));
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("OTHER", "SampleAgg", 12)
        ));
    }

    @Test
    public void reserveSequenceNumbersInvalidDoubleSequence() {
        try {
            testSubject.reserveSequenceNumbers(asList(
                    serializedEvent("OTHER", "SampleAgg", 11),
                    serializedEvent("OTHER", "SampleAgg", 11)
            ));
            fail("Should fail, as second sequence number should be 12");
        } catch (MessagingPlatformException mpe) {
            assertEquals(ErrorCode.INVALID_SEQUENCE, mpe.getErrorCode());
        }
    }

    @Test
    public void reserveSequenceNumbersInvalidGapSequence() {
        try {
            testSubject.reserveSequenceNumbers(asList(
                    serializedEvent("OTHER", "SampleAgg", 11),
                    serializedEvent("OTHER", "SampleAgg", 13)
            ));
            fail("Should fail, as second sequence number should be 12");
        } catch (MessagingPlatformException mpe) {
            assertEquals(ErrorCode.INVALID_SEQUENCE, mpe.getErrorCode());
        }
    }

    @Test
    public void clearOldValues() {
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("OTHER", "SampleAgg", 11)));
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("SECOND_OTHER", "SampleAgg", 11)));

        clock.timeElapses(11);
        testSubject.clearOld(10);
        testSubject.reserveSequenceNumbers(asList(
                serializedEvent("OTHER", "SampleAgg", 11)));
    }

    private Event serializedEvent(String aggregateId, String aggregateType, int sequenceNumber) {
        return Event.newBuilder().setAggregateIdentifier(aggregateId)
                    .setAggregateType(aggregateType)
                    .setAggregateSequenceNumber(sequenceNumber)
                    .build();
    }
}