package io.axoniq.sample;

import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.model.AggregateIdentifier;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.spring.stereotype.Aggregate;
import org.springframework.context.annotation.Profile;

import static org.axonframework.commandhandling.model.AggregateLifecycle.apply;

/**
 * Author: marc
 */
@Aggregate
@Profile("commands")
public class SampleCommandHandler {

    @AggregateIdentifier
    private String id;

    @CommandHandler
    public SampleCommandHandler(EchoCommand command) {
        if( "Hello".equals(command.getText())) throw new NullPointerException();
        apply(new EchoEvent(command.getId(), command.getText()));
    }
    public SampleCommandHandler() {

    }

    @EventSourcingHandler
    public void handle(EchoEvent echoEvent) {
        this.id = echoEvent.getId();
    }

}
