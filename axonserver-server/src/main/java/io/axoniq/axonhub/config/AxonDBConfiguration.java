package io.axoniq.axonhub.config;

import io.axoniq.platform.grpc.NodeInfo;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Author: marc
 */
@Configuration
@ConfigurationProperties(prefix = "axoniq.axondb")
public class AxonDBConfiguration {
    private static final int DEFAULT_AXONDB_PORT = 8123;
    private String servers;
    private String token;
    private String certFile;
    private boolean sslEnabled;
    private long keepAliveTimeout = 5000;
    private long keepAliveTime = 0;

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getCertFile() {
        return certFile;
    }

    public void setCertFile(String certFile) {
        this.certFile = certFile;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public List<NodeInfo> getServerNodes() {
        List<NodeInfo> serverNodes = new ArrayList<>();
        if (servers != null) {
            String[] serverArr = servers.split(",");
            Arrays.stream(serverArr).forEach(serverString -> {
                String[] hostPort = serverString.trim().split(":");
                if( hostPort.length == 1) {
                    NodeInfo nodeInfo = NodeInfo.newBuilder().setHostName(hostPort[0])
                            .setGrpcPort(DEFAULT_AXONDB_PORT)
                            .build();
                    serverNodes.add(nodeInfo);
                } else {
                    NodeInfo nodeInfo = NodeInfo.newBuilder().setHostName(hostPort[0])
                            .setGrpcPort(Integer.valueOf(hostPort[1]))
                            .build();
                    serverNodes.add(nodeInfo);
                }
            });
        }
        return serverNodes;
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    public long getKeepAliveTimeout() {

        return keepAliveTimeout;
    }

    public void setKeepAliveTimeout(long keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }
}
