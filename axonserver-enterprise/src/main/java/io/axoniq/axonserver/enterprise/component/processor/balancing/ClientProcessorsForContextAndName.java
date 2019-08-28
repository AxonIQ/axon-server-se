package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.function.Predicate;

import static java.util.stream.StreamSupport.stream;

/**
 * Implementation of {@link ClientProcessors} which returns a filtered stream based on a {@link ClientProcessor} its
 * {@code context} and {@code processorName}.
 *
 * @author Steven van Beelen
 * @since 4.2.1
 */
public class ClientProcessorsForContextAndName implements ClientProcessors {

    private final ClientProcessors allEventProcessors;
    private final Predicate<ClientProcessor> belongsToContextAndHasProcessorName;

    /**
     * Build a {@link ClientProcessors} implementation which returns a filtered stream based on a
     * {@link ClientProcessor} its {@code context} and {@code processorName}.
     *
     * @param allEventProcessors a {@link ClientProcessors} implementation containing all known {@link ClientProcessor}s
     * @param context            the {@code context} which a {@link ClientProcessor} should match with
     * @param processorName      the {@code processorName} which a {@link ClientProcessor} should match with
     */
    public ClientProcessorsForContextAndName(ClientProcessors allEventProcessors,
                                             String context,
                                             String processorName) {
        this(allEventProcessors, new ProcessorBelongsToContextAndHasProcessorName(context, processorName));
    }

    /**
     * Build a {@link ClientProcessors} implementation which returns a filtered stream based on a
     * {@link ClientProcessor} matching the given {@link Predicate}.
     *
     * @param allEventProcessors                  a {@link ClientProcessors} implementation containing all known
     *                                            {@link ClientProcessor}s
     * @param belongsToContextAndHasProcessorName a {@link Predicate} used to filter {@link ClientProcessor}s that
     *                                            belong to a given {@code context} and {@code processorName}
     */
    public ClientProcessorsForContextAndName(ClientProcessors allEventProcessors,
                                             Predicate<ClientProcessor> belongsToContextAndHasProcessorName) {
        this.belongsToContextAndHasProcessorName = belongsToContextAndHasProcessorName;
        this.allEventProcessors = allEventProcessors;
    }

    @NotNull
    @Override
    public Iterator<ClientProcessor> iterator() {
        return stream(allEventProcessors.spliterator(), false).filter(belongsToContextAndHasProcessorName).iterator();
    }

    /**
     * A simple {@link Predicate} implementation which tests a given {@link ClientProcessor} to match the configured
     * {@code context} and {@code processorName}.
     */
    private static class ProcessorBelongsToContextAndHasProcessorName implements Predicate<ClientProcessor> {

        private final String context;
        private final String processorName;

        private ProcessorBelongsToContextAndHasProcessorName(String context, String processorName) {
            this.context = context;
            this.processorName = processorName;
        }

        @Override
        public boolean test(ClientProcessor clientProcessor) {
            return clientProcessor.belongsToContext(context) &&
                    clientProcessor.eventProcessorInfo().getProcessorName().equals(processorName);
        }
    }
}
