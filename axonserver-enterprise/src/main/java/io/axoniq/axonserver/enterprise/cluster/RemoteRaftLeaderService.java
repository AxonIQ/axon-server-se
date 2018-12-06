package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public class RemoteRaftLeaderService implements RaftLeaderService {

    private final String context;
    private final RaftGroupServiceGrpc.RaftGroupServiceStub stub;

    public RemoteRaftLeaderService(String context, RaftGroupServiceGrpc.RaftGroupServiceStub stub) {
        this.context = context;
        this.stub = stub;
    }

    @Override
    public CompletableFuture<Void> addNodeToContext(Node me) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ContextMember contextMember = ContextMember.newBuilder()
                                                   .setHost(me.getHost())
                                                   .setNodeId(me.getNodeId())
                                                   .setPort(me.getPort())
                                                   .build();
        stub.addNodeToContext(Context.newBuilder().setName(context).addMembers(contextMember).build(),
                              new StreamObserver<Confirmation>() {
                                  @Override
                                  public void onNext(Confirmation confirmation) {
                                      result.complete(null);
                                  }

                                  @Override
                                  public void onError(Throwable throwable) {
                                      result.completeExceptionally(throwable);
                                  }

                                  @Override
                                  public void onCompleted() {
                                      // Ignore, should already be completed
                                  }
                              });
        return result;
    }
}
