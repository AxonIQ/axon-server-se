package io.axoniq.axonhub.grpc;

import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.Group;
import io.axoniq.axonhub.internal.grpc.InternalFlowControl;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Author: marc
 */
public class ClusterFlowControlStreamObserver extends SendingStreamObserver<ConnectorCommand> {

    private ConnectorCommand newCommandPermitsRequest;
    private final AtomicLong remainingCommandPermits = new AtomicLong();
    private volatile long newCommandPermits;

    private ConnectorCommand newQueryPermitsRequest;
    private final AtomicLong remainingQueryPermits = new AtomicLong();
    private volatile long newQueryPermits;

    public ClusterFlowControlStreamObserver(StreamObserver<ConnectorCommand> delegate) {
        super(delegate);

    }

    @Override
    public void onNext(ConnectorCommand t) {
        super.onNext(t);
        switch( t.getRequestCase()) {
            case COMMAND_RESPONSE:
                if( remainingCommandPermits.decrementAndGet() == 0 ) {
                    super.onNext(newCommandPermitsRequest);
                    remainingCommandPermits.addAndGet(newCommandPermits);
                }
                break;
            case QUERY_RESPONSE:
                if( remainingQueryPermits.decrementAndGet() == 0 ) {
                    super.onNext(newQueryPermitsRequest);
                    remainingCommandPermits.addAndGet(newQueryPermits);
                }
                break;
            default:
                break;
        }
    }

    public void initCommandFlowControl(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        remainingCommandPermits.set(messagingPlatformConfiguration.getCommandFlowControl().getInitialPermits()-
                messagingPlatformConfiguration.getCommandFlowControl().getThreshold());
        this.newCommandPermits = messagingPlatformConfiguration.getCommandFlowControl().getNewPermits();
        newCommandPermitsRequest = ConnectorCommand.newBuilder().setFlowControl(
                InternalFlowControl.newBuilder().setNodeName(messagingPlatformConfiguration.getName())
                        .setGroup(Group.COMMAND)
                        .setPermits(messagingPlatformConfiguration.getCommandFlowControl().getNewPermits()).build()).build();
        onNext(ConnectorCommand.newBuilder().setFlowControl(InternalFlowControl.newBuilder(newCommandPermitsRequest.getFlowControl())
                .setPermits(messagingPlatformConfiguration.getCommandFlowControl().getInitialPermits()).build()).build());
    }
    public void initQueryFlowControl(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        remainingQueryPermits.set(messagingPlatformConfiguration.getQueryFlowControl().getInitialPermits()-
                messagingPlatformConfiguration.getQueryFlowControl().getThreshold());
        this.newQueryPermits = messagingPlatformConfiguration.getQueryFlowControl().getNewPermits();
        newQueryPermitsRequest = ConnectorCommand.newBuilder().setFlowControl(
                InternalFlowControl.newBuilder().setNodeName(messagingPlatformConfiguration.getName())
                        .setGroup(Group.QUERY)
                        .setPermits(messagingPlatformConfiguration.getQueryFlowControl().getNewPermits()).build()).build();
        onNext(ConnectorCommand.newBuilder().setFlowControl(InternalFlowControl.newBuilder(newQueryPermitsRequest.getFlowControl())
                .setPermits(messagingPlatformConfiguration.getQueryFlowControl().getInitialPermits()).build()).build());
    }
}
