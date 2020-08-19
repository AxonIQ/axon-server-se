package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.instance.FakeClient;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;

import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 * @since
 */
public class ClientsByEventProcessorTest {

    public static final String TOKEN_STORE_1 = "1";
    public static final String TOKEN_STORE_2 = "2";
    private static final String CLIENT_A = "clientA";
    private static final String CLIENT_B = "clientB";
    private static final String CLIENT_C = "clientC";
    private static final String CLIENT_D = "clientD";
    private static final String CLIENT_E = "clientE";
    private static final String CLIENT_F = "clientF";
    private static final boolean BELONGS_TO_COMPONENT = true;
    private static final boolean DOES_NOT_BELONG_TO_COMPONENT = false;
    private static final boolean BELONGS_TO_CONTEXT = true;
    private static final boolean DOES_NOT_BELONG_TO_CONTEXT = false;
    private static final String BLUE_PROCESSOR = "Blue";
    private static final String GREEN_PROCESSOR = "Green";
    private static final String RED_PROCESSOR = "Red";
    private final EventProcessorIdentifier blue1 = new EventProcessorIdentifier(BLUE_PROCESSOR, TOKEN_STORE_1);
    private final EventProcessorIdentifier green1 = new EventProcessorIdentifier(GREEN_PROCESSOR, TOKEN_STORE_1);
    private final EventProcessorIdentifier red1 = new EventProcessorIdentifier(RED_PROCESSOR, TOKEN_STORE_1);

    private final EventProcessorIdentifier blue2 = new EventProcessorIdentifier(BLUE_PROCESSOR, TOKEN_STORE_2);
    private final EventProcessorIdentifier green2 = new EventProcessorIdentifier(GREEN_PROCESSOR, TOKEN_STORE_2);
    private final EventProcessorIdentifier red2 = new EventProcessorIdentifier(RED_PROCESSOR, TOKEN_STORE_2);

    private final EventProcessorInfo blue1Info = EventProcessorInfo.newBuilder()
                                                                   .setProcessorName(BLUE_PROCESSOR)
                                                                   .setTokenStoreIdentifier(TOKEN_STORE_1)
                                                                   .build();
    private final EventProcessorInfo green1Info = EventProcessorInfo.newBuilder()
                                                                    .setProcessorName(GREEN_PROCESSOR)
                                                                    .setTokenStoreIdentifier(TOKEN_STORE_1)
                                                                    .build();
    private final EventProcessorInfo red1Info = EventProcessorInfo.newBuilder()
                                                                  .setProcessorName(RED_PROCESSOR)
                                                                  .setTokenStoreIdentifier(TOKEN_STORE_1)
                                                                  .build();
    private final EventProcessorInfo blue2Info = EventProcessorInfo.newBuilder()
                                                                   .setProcessorName(BLUE_PROCESSOR)
                                                                   .setTokenStoreIdentifier(TOKEN_STORE_2)
                                                                   .build();
    private final EventProcessorInfo green2Info = EventProcessorInfo.newBuilder()
                                                                    .setProcessorName(GREEN_PROCESSOR)
                                                                    .setTokenStoreIdentifier(TOKEN_STORE_2)
                                                                    .build();
    private final EventProcessorInfo red2Info = EventProcessorInfo.newBuilder()
                                                                  .setProcessorName(RED_PROCESSOR)
                                                                  .setTokenStoreIdentifier(TOKEN_STORE_2)
                                                                  .build();


    private final ClientProcessor blueA =
            new FakeClientProcessor(CLIENT_A, BELONGS_TO_COMPONENT, BELONGS_TO_CONTEXT, blue1Info);
    private final ClientProcessor greenA =
            new FakeClientProcessor(CLIENT_A, BELONGS_TO_COMPONENT, BELONGS_TO_CONTEXT, green1Info);
    private final ClientProcessor redA =
            new FakeClientProcessor(CLIENT_A, BELONGS_TO_COMPONENT, BELONGS_TO_CONTEXT, red1Info);

    private final ClientProcessor blueB =
            new FakeClientProcessor(CLIENT_B, DOES_NOT_BELONG_TO_COMPONENT, BELONGS_TO_CONTEXT, blue1Info);
    private final ClientProcessor greenB =
            new FakeClientProcessor(CLIENT_B, DOES_NOT_BELONG_TO_COMPONENT, BELONGS_TO_CONTEXT, green1Info);
    private final ClientProcessor redB =
            new FakeClientProcessor(CLIENT_B, DOES_NOT_BELONG_TO_COMPONENT, BELONGS_TO_CONTEXT, red1Info);

    private final ClientProcessor blueC =
            new FakeClientProcessor(CLIENT_C, DOES_NOT_BELONG_TO_COMPONENT, DOES_NOT_BELONG_TO_CONTEXT, blue1Info);
    private final ClientProcessor greenC =
            new FakeClientProcessor(CLIENT_C, DOES_NOT_BELONG_TO_COMPONENT, DOES_NOT_BELONG_TO_CONTEXT, green1Info);
    private final ClientProcessor redC =
            new FakeClientProcessor(CLIENT_C, DOES_NOT_BELONG_TO_COMPONENT, DOES_NOT_BELONG_TO_CONTEXT, red1Info);

    private final ClientProcessor blueD =
            new FakeClientProcessor(CLIENT_D, BELONGS_TO_COMPONENT, BELONGS_TO_CONTEXT, blue2Info);
    private final ClientProcessor greenD =
            new FakeClientProcessor(CLIENT_D, BELONGS_TO_COMPONENT, BELONGS_TO_CONTEXT, green2Info);
    private final ClientProcessor redD =
            new FakeClientProcessor(CLIENT_D, BELONGS_TO_COMPONENT, BELONGS_TO_CONTEXT, red2Info);

    private final ClientProcessor blueE =
            new FakeClientProcessor(CLIENT_E, DOES_NOT_BELONG_TO_COMPONENT, BELONGS_TO_CONTEXT, blue2Info);
    private final ClientProcessor greenE =
            new FakeClientProcessor(CLIENT_E, DOES_NOT_BELONG_TO_COMPONENT, BELONGS_TO_CONTEXT, green2Info);
    private final ClientProcessor redE =
            new FakeClientProcessor(CLIENT_E, DOES_NOT_BELONG_TO_COMPONENT, BELONGS_TO_CONTEXT, red2Info);

    private final ClientProcessor blueF =
            new FakeClientProcessor(CLIENT_F, DOES_NOT_BELONG_TO_COMPONENT, DOES_NOT_BELONG_TO_CONTEXT, blue2Info);
    private final ClientProcessor greenF =
            new FakeClientProcessor(CLIENT_F, DOES_NOT_BELONG_TO_COMPONENT, DOES_NOT_BELONG_TO_CONTEXT, green2Info);
    private final ClientProcessor redF =
            new FakeClientProcessor(CLIENT_F, DOES_NOT_BELONG_TO_COMPONENT, DOES_NOT_BELONG_TO_CONTEXT, red2Info);


    private final ClientProcessors clientProcessors = () -> asList(blueA, redA, greenA,
                                                                   blueB, redB, greenB,
                                                                   blueC, redC, greenC,
                                                                   blueD, redD, greenD,
                                                                   blueE, redE, greenE,
                                                                   blueF, redF, greenF
    ).iterator();

    private final Client clientA = new FakeClient(CLIENT_A, "context", true);
    private final Client clientB = new FakeClient(CLIENT_B, "context", false);
    private final Client clientC = new FakeClient(CLIENT_C, "anotherContext", false);
    private final Client clientD = new FakeClient(CLIENT_D, "context", true);
    private final Client clientE = new FakeClient(CLIENT_E, "context", false);
    private final Client clientF = new FakeClient(CLIENT_F, "anotherContext", false);

    private final Clients clients = () -> asList(clientA, clientB, clientC, clientD, clientE, clientF).iterator();

    @Test
    public void testIterator() {
        ClientsByEventProcessor testSubject = new ClientsByEventProcessor(blue1,
                                                                          "context",
                                                                          clientProcessors);
        Iterator<String> iterator = testSubject.iterator();
        assertEquals(CLIENT_A, iterator.next());
        assertEquals(CLIENT_B, iterator.next());
        assertFalse(iterator.hasNext());

        ClientsByEventProcessor testSubject2 = new ClientsByEventProcessor(red2,
                                                                           "anotherContext",
                                                                           clientProcessors);
        Iterator<String> iterator2 = testSubject2.iterator();
        assertFalse(iterator2.hasNext());

        ClientsByEventProcessor testSubject3 = new ClientsByEventProcessor(green2,
                                                                           "context",
                                                                           clientProcessors);
        Iterator<String> iterator3 = testSubject3.iterator();
        assertEquals(CLIENT_D, iterator3.next());
        assertEquals(CLIENT_E, iterator3.next());
        assertFalse(iterator3.hasNext());
    }
}