package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.TermIndex;
import org.junit.*;

/**
 * @author Marc Gathier
 */
public class PrintLogEntries {

    private FileSegmentLogEntryStore testSubject;
    private PrimaryLogEntryStore primary;


    @Test
    public void printNode1() {
        primary = PrimaryEventStoreFactory.create("D:\\test\\axonserver\\axonserver-enterprise-4.1.3\\axonserver1\\log", "_admin");
        testSubject = new FileSegmentLogEntryStore("_admin", primary);
        testSubject.createIterator(testSubject.firstLogIndex()).forEachRemaining(e -> {
            System.out.printf( "index: %s, term %s: type %s%n", e.getIndex(), e.getTerm(), e.hasNewConfiguration() ? "New Configuration" : e.getSerializedObject().getType());
        });
        TermIndex lastLog = testSubject.lastLog();
        System.out.printf( "index: %s, term %s%n", lastLog.getIndex(), lastLog.getTerm());
    }

    @Test
    public void printNode3() {
        primary = PrimaryEventStoreFactory.create("D:\\test\\axonserver\\axonserver-enterprise-4.1.3\\axonserver3\\log", "_admin");
        testSubject = new FileSegmentLogEntryStore("_admin", primary);
        testSubject.createIterator(testSubject.firstLogIndex()).forEachRemaining(e -> {
            System.out.printf( "index: %s, term %s: type %s%n", e.getIndex(), e.getTerm(), e.hasNewConfiguration() ? "New Configuration" : e.getSerializedObject().getType());
        });
        TermIndex lastLog = testSubject.lastLog();
        System.out.printf( "index: %s, term %s%n", lastLog.getIndex(), lastLog.getTerm());
    }

    @Test
    public void printNode2() {
        primary = PrimaryEventStoreFactory.create("D:\\test\\axonserver\\axonserver-enterprise-4.1.3\\axonserver2\\log", "demo08");
        primary.initSegments(Long.MAX_VALUE);
        testSubject = new FileSegmentLogEntryStore("demo08", primary);
        testSubject.createIterator(testSubject.firstLogIndex()).forEachRemaining(e -> {
            System.out.printf( "index: %s, term %s: type %s%n", e.getIndex(), e.getTerm(), e.hasNewConfiguration() ? "New Configuration" : e.getSerializedObject().getType());
        });
        TermIndex lastLog = testSubject.lastLog();
        System.out.printf( "index: %s, term %s%n", lastLog.getIndex(), lastLog.getTerm());
    }

}
