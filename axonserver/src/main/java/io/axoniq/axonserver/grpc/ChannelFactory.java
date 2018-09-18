package io.axoniq.axonserver.grpc;


import io.axoniq.platform.grpc.NodeInfo;
import io.grpc.Channel;

/**
 * Created by Sara Pellegrini on 20/04/2018.
 * sara.pellegrini@gmail.com
 */
public interface ChannelFactory<T extends Channel> {

    T create(String hostName, int port);

    default T create(NodeInfo nodeInfo){
        return create(nodeInfo.getHostName(), nodeInfo.getGrpcPort());
    }

}
