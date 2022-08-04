package io.axoniq.axonserver.commandprocesing.imp;

import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class MultiQueueListenerTest {

    private final MultiQueueListener<String> testSubject = new MultiQueueListener<>();

    @Test
    public void send() throws InterruptedException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.execute(testSubject::sender);
        testSubject.addQueue("sample", 0);
        testSubject.addQueue("sample2", 10);
        testSubject.enqueue("sample", "message1");
        testSubject.enqueue("sample2", "message12");
        Thread.sleep(10);
        testSubject.request("sample", 10);
        Thread.sleep(10);
        testSubject.enqueue("sample", "message2");
        testSubject.enqueue("sample", "message3");
        executor.shutdown();
    }
}