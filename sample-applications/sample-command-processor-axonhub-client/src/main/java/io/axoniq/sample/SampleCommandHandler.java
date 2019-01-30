package io.axoniq.sample;

import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.model.AggregateIdentifier;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.spring.stereotype.Aggregate;

import static org.axonframework.commandhandling.model.AggregateLifecycle.apply;
/**
 * @author Marc Gathier
 */
@Aggregate
public class SampleCommandHandler {

    @AggregateIdentifier
    private String id;

    @CommandHandler
    public SampleCommandHandler(EchoCommand command) {
        apply(new EchoEvent(command.getId(), command.getText()));
    }
    public SampleCommandHandler() {

    }

    @CommandHandler
    public String update(UpdateCommand updateCommand) {
        if( updateCommand.getText().equalsIgnoreCase("hello")) throw new IllegalArgumentException("Text cannot be hello");
        return updateCommand.getText();
    }

    @EventSourcingHandler
    public void handle(EchoEvent echoEvent) {
        this.id = echoEvent.getId();
    }

}
