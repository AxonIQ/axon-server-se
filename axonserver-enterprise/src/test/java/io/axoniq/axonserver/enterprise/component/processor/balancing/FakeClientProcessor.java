package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

/**
 * Fake {@link ClientProcessor} implementation for test purposes. Copied from Axon Server SE.
 *
 * @author Sara Pellegrini
 * @author Steven van Beelen
 */
public class FakeClientProcessor implements ClientProcessor {

    private static final boolean BELONGS_TO_COMPONENT = true;

    private final String clientId;
    private final boolean belongsToContext;
    private final EventProcessorInfo eventProcessorInfo;

    /**
     * Build a fake {@link ClientProcessor} with the given {@code clientId}, returning {@code belongsToContext} when
     * {@link #belongsToComponent(String)} is called and using the given {@code processorName} to create an
     * {@link EventProcessorInfo}.
     *
     * @param clientId         the id of the fake {@link ClientProcessor}
     * @param belongsToContext the result of calling {@link #belongsToComponent(String)}
     * @param processorName    the name of the {@link EventProcessorInfo} to set
     */
    public FakeClientProcessor(String clientId, boolean belongsToContext, String processorName) {
        this(clientId, belongsToContext, processorInfoWithName(processorName));
    }

    /**
     * Build a fake {@link ClientProcessor} with the given {@code clientId} and {@code eventProcessorInfo}, which
     * returns {@code belongsToContext} when {@link #belongsToComponent(String)} is called.
     *
     * @param clientId           the id of the fake {@link ClientProcessor}
     * @param belongsToContext   the result of calling {@link #belongsToComponent(String)}
     * @param eventProcessorInfo the {@link EventProcessorInfo} of the fake {@link ClientProcessor}
     */
    public FakeClientProcessor(String clientId, boolean belongsToContext, EventProcessorInfo eventProcessorInfo) {
        this.clientId = clientId;
        this.belongsToContext = belongsToContext;
        this.eventProcessorInfo = eventProcessorInfo;
    }

    private static EventProcessorInfo processorInfoWithName(String processorName) {
        return EventProcessorInfo.newBuilder()
                                 .setProcessorName(processorName)
                                 .build();
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public EventProcessorInfo eventProcessorInfo() {
        return eventProcessorInfo;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return BELONGS_TO_COMPONENT;
    }

    @Override
    public boolean belongsToContext(String context) {
        return belongsToContext;
    }
}
