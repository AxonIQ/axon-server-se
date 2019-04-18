package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.config.FlowControl;
import io.axoniq.axonserver.grpc.SendingStreamObserver;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.Group;
import io.axoniq.axonserver.grpc.internal.InternalFlowControl;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicLong;


/**
 * @author Marc Gathier
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
                    remainingQueryPermits.addAndGet(newQueryPermits);
                }
                break;
            default:
                break;
        }
    }

    public void initCommandFlowControl(String name, FlowControl flowControl) {
        remainingCommandPermits.set(flowControl.getInitialPermits()-
                                            flowControl.getThreshold());
        this.newCommandPermits = flowControl.getNewPermits();
        newCommandPermitsRequest = ConnectorCommand.newBuilder().setFlowControl(
                InternalFlowControl.newBuilder().setNodeName(name)
                        .setGroup(Group.COMMAND)
                        .setPermits(flowControl.getNewPermits()).build()).build();
        onNext(ConnectorCommand.newBuilder().setFlowControl(InternalFlowControl.newBuilder(newCommandPermitsRequest.getFlowControl())
                .setPermits(flowControl.getInitialPermits()).build()).build());
    }
    public void initQueryFlowControl(String name, FlowControl flowControl) {
        remainingQueryPermits.set(flowControl.getInitialPermits()-
                                          flowControl.getThreshold());
        this.newQueryPermits = flowControl.getNewPermits();
        newQueryPermitsRequest = ConnectorCommand.newBuilder().setFlowControl(
                InternalFlowControl.newBuilder().setNodeName(name)
                        .setGroup(Group.QUERY)
                        .setPermits(flowControl.getNewPermits()).build()).build();
        onNext(ConnectorCommand.newBuilder().setFlowControl(InternalFlowControl.newBuilder(newQueryPermitsRequest.getFlowControl())
                .setPermits(flowControl.getInitialPermits()).build()).build());
    }
}
