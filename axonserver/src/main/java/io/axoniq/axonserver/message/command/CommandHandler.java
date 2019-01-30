package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.message.ClientIdentification;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public abstract class CommandHandler<T> implements Comparable<CommandHandler<T>> {
    protected final StreamObserver<T> observer;
    protected final ClientIdentification client;
    protected final String componentName;

    public CommandHandler(StreamObserver<T> responseObserver, ClientIdentification client, String componentName) {
        this.observer = responseObserver;
        this.client = client;
        this.componentName = componentName;
    }

    public ClientIdentification getClient() {
        return client;
    }

    public String getComponentName() {
        return componentName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandHandler<?> that = (CommandHandler<?>) o;
        return Objects.equals(observer, that.observer) &&
                Objects.equals(client, that.client);
    }

    @Override
    public int hashCode() {
        return Objects.hash(observer, client);
    }

    @Override
    public int compareTo(CommandHandler o) {
        int clientResult = client.compareTo(o.client);
        if( clientResult == 0) {
            clientResult = observer.toString().compareTo(o.observer.toString());
        }
        return clientResult;
    }

    public abstract void dispatch( SerializedCommand request);

    public abstract void confirm( String messageId);

    public String queueName() {
        return client.toString();
    }

    public String getMessagingServerName() {
        return null;
    }
}
