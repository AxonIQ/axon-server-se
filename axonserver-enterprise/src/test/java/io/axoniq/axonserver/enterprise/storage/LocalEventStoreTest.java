package io.axoniq.axonserver.enterprise.storage;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.rules.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;

/**
 * Author: marc
 */
public class LocalEventStoreTest {
    private LocalEventStore testSubject;
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void init()  {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {});
        embeddedDBProperties.getEvent().setStorage(tempFolder.getRoot().getAbsolutePath());
        embeddedDBProperties.getEvent().setSegmentSize(512*1024L);
        embeddedDBProperties.getEvent().setForceInterval(100);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        EventStoreFactory eventStoreFactory = new DatafileEventStoreFactory(embeddedDBProperties, new DefaultEventTransformerFactory(),
                                                                            new DefaultStorageTransactionManagerFactory());

        testSubject = new LocalEventStore(eventStoreFactory);
//        testSubject = new EmbeddedEventStore(new JdbcEventStoreFactory(new StorageProperties()));
        testSubject.initContext("default", false);
    }

    @After
    public void close() {
        testSubject.cleanupContext("default");
    }


    @Test
    public void testParallelTransactions() {
        String[] results = new String[2];
        StreamObserver<InputStream> inputStream1 = testSubject.createAppendEventConnection("default",
                                                                                           new StreamObserver<Confirmation>() {
                                                                                               @Override
                                                                                               public void onNext(
                                                                                                       Confirmation confirmation) {

                                                                                               }

                                                                                               @Override
                                                                                               public void onError(
                                                                                                       Throwable throwable) {
                                                                                                   results[0] = throwable.getMessage();

                                                                                               }

                                                                                               @Override
                                                                                               public void onCompleted() {
                                                                                                   results[0] = "OK";
                                                                                               }
                                                                                           });
        StreamObserver<InputStream> inputStream2 = testSubject.createAppendEventConnection("default",
                                                                                           new StreamObserver<Confirmation>() {
                                                                                               @Override
                                                                                               public void onNext(
                                                                                                       Confirmation confirmation) {

                                                                                               }

                                                                                               @Override
                                                                                               public void onError(
                                                                                                       Throwable throwable) {

                                                                                                   results[1] = throwable.getMessage();
                                                                                               }

                                                                                               @Override
                                                                                               public void onCompleted() {
                                                                                                   results[1] = "OK";
                                                                                               }
                                                                                           });

        Event event = Event.newBuilder().setAggregateIdentifier("1").setAggregateSequenceNumber(0).build();

        inputStream1.onNext(new ByteArrayInputStream(event.toByteArray()));
        inputStream2.onNext(new ByteArrayInputStream(event.toByteArray()));

        inputStream1.onCompleted();
        inputStream2.onCompleted();

        Assert.assertEquals("OK", results[0]);
        Assert.assertNotEquals("OK", results[1]);
    }

    @Test
    public void count() throws InterruptedException {
        CountDownLatch storeLatch = new CountDownLatch(1);
        StreamObserver<InputStream> inputStream1 = testSubject.createAppendEventConnection("default",
                                                                                     new StreamObserver<Confirmation>() {
                                                                                         @Override
                                                                                         public void onNext(
                                                                                                 Confirmation confirmation) {
                                                                                             storeLatch.countDown();
                                                                                         }

                                                                                         @Override
                                                                                         public void onError(
                                                                                                 Throwable throwable) {
                                                                                             storeLatch.countDown();
                                                                                         }

                                                                                         @Override
                                                                                         public void onCompleted() {
                                                                                         }
                                                                                     });
        Event event = Event.newBuilder().setAggregateIdentifier("1").setAggregateSequenceNumber(0).build();

        inputStream1.onNext(new ByteArrayInputStream(event.toByteArray()));
        inputStream1.onCompleted();

        storeLatch.await(1, TimeUnit.SECONDS);



        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<QueryEventsRequest> requestStream = testSubject.queryEvents("default",
                                                                                   new StreamObserver<QueryEventsResponse>() {
                                                                                       @Override
                                                                                       public void onNext(
                                                                                               QueryEventsResponse queryEventsResponse) {
                                                                                           if(queryEventsResponse.getFilesCompleted().getSuccess()) {
                                                                                                latch.countDown();
                                                                                           } else {
                                                                                               System.out.println(
                                                                                                       queryEventsResponse);
                                                                                           }
                                                                                       }

                                                                                       @Override
                                                                                       public void onError(
                                                                                               Throwable throwable) {

                                                                                       }

                                                                                       @Override
                                                                                       public void onCompleted() {

                                                                                       }
                                                                                   });

        requestStream.onNext(QueryEventsRequest.newBuilder()
                                               .setNumberOfPermits(100000)
                                               .setQuery("count()")
                                               .build());
        latch.await(1, TimeUnit.SECONDS);
    }


    @Test
    public void testAvailable() throws InterruptedException {
        Event demoEvent = Event.newBuilder().setAggregateIdentifier("DEMO").setAggregateSequenceNumber(0)
                               .setAggregateType("DemoType").build();
        long last = testSubject.getLastToken(Topology.DEFAULT_CONTEXT);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        StreamObserver<InputStream> connection = testSubject.createAppendEventConnection(Topology.DEFAULT_CONTEXT,
                                                                                         new StreamObserver<Confirmation>() {
                                                                                             @Override
                                                                                             public void onNext(
                                                                                                     Confirmation confirmation) {
                                                                                                 countDownLatch.countDown();
                                                                                             }

                                                                                             @Override
                                                                                             public void onError(
                                                                                                     Throwable throwable) {

                                                                                             }

                                                                                             @Override
                                                                                             public void onCompleted() {

                                                                                             }
                                                                                         });
        connection.onNext(new ByteArrayInputStream(demoEvent.toByteArray()));
        connection.onCompleted();

        countDownLatch.await();

        assertTrue(testSubject.containsEvents(Topology.DEFAULT_CONTEXT, new SerializedTransactionWithToken(last+1, 1, Collections.singletonList(new SerializedEvent(demoEvent)))));


    }
}