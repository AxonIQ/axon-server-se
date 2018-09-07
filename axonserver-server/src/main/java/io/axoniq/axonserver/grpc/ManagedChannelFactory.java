package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.config.AxonDBConfiguration;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;

import java.io.File;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

/**
 * Created by Sara Pellegrini on 20/04/2018.
 * sara.pellegrini@gmail.com
 */
public class ManagedChannelFactory implements ChannelFactory<ManagedChannel>{

    private final boolean sslEnabled;
    private final long keepAliveTime;
    private final long keepAliveTimeout;

    private final String certFilePathName;

    public ManagedChannelFactory(AxonDBConfiguration connectInformation) {
        this.sslEnabled = connectInformation.isSslEnabled();
        this.certFilePathName = connectInformation.getCertFile();
        this.keepAliveTime = connectInformation.getKeepAliveTime();
        this.keepAliveTimeout = connectInformation.getKeepAliveTimeout();
    }

    public ManagedChannelFactory(boolean sslEnabled, String certFilePathName) {
        this.sslEnabled = sslEnabled;
        this.certFilePathName = certFilePathName;
        this.keepAliveTime = 1000;
        this.keepAliveTimeout = 5000;
    }

    public ManagedChannel create(String hostName, int port){
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(hostName, port);

        if( keepAliveTime > 0) {
            builder.keepAliveTime(keepAliveTime, TimeUnit.MILLISECONDS)
                   .keepAliveTimeout(keepAliveTimeout, TimeUnit.MILLISECONDS)
                   .keepAliveWithoutCalls(true);
        }

        if (sslEnabled) {
            try {
                if(certFilePathName != null) {
                    File certFile = new File(certFilePathName);
                    if (!certFile.exists()) {
                        throw new RuntimeException("Certificate file " + certFile + " does not exist");
                    }
                    SslContext sslContext = GrpcSslContexts.forClient()
                                                           .trustManager(new File(certFilePathName))
                                                           .build();
                    builder.sslContext(sslContext);
                }
            } catch (SSLException e) {
                throw new RuntimeException("Couldn't set up SSL context", e);
            }
        } else {
            builder.usePlaintext();
        }
        return builder.build();
    }

}
