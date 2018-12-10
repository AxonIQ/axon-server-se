package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class RemoteRaftGroupService implements RaftGroupService {

    private final RaftGroupServiceGrpc.RaftGroupServiceStub stub;

    public RemoteRaftGroupService( RaftGroupServiceGrpc.RaftGroupServiceStub stub) {
        this.stub = stub;
    }

    @Override
    public CompletableFuture<Void> addNodeToContext(String context, Node node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ContextMember contextMember = ContextMember.newBuilder()
                                                   .setHost(node.getHost())
                                                   .setNodeId(node.getNodeId())
                                                   .setPort(node.getPort())
                                                   .build();
        stub.addServer(Context.newBuilder().setName(context).addMembers(contextMember).build(),
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

    @Override
    public CompletableFuture<Void> deleteNode(String context, String node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.removeServer(Context.newBuilder().setName(context)
                                          .addMembers(ContextMember.newBuilder().setNodeId(node).build()).build(),
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

                                       }
                                   });
        return result;
    }

    @Override
    public void getStatus(Consumer<Context> contextConsumer) {
        stub.getStatus(Context.getDefaultInstance(), new StreamObserver<Context>() {
            @Override
            public void onNext(Context context) {
                contextConsumer.accept(context);
            }

            @Override
            public void onError(Throwable throwable) {
                // log only
            }

            @Override
            public void onCompleted() {
                // no further action needed
            }
        });

    }

    @Override
    public CompletableFuture<Void> initContext(String context, List<Node> raftNodes) {
        CompletableFuture<Void> result = new CompletableFuture<>();
            Context request = Context.newBuilder()
                                     .setName(context)
                                     .addAllMembers(raftNodes.stream().map(r -> ContextMember.newBuilder()
                                                                                   .setHost(r.getHost())
                                                                           .setPort(r.getPort())
                                                                           .setNodeId(r.getNodeId())
                                                                           .build()
                                             ).collect(Collectors.toList()))
                                     .build();
        stub.initContext(
                    request,
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
                            // no action required, already handled by confirmation
                        }
                    });

        return result;
    }
}
