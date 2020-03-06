package io.axoniq.axonserver.enterprise.init;

import io.axoniq.axonserver.KeepNames;

/**
 * Defines a new definition of a node in an AxonServer cluster. To rename an existing node use {@code oldName} as the
 * existing node name.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@KeepNames
public class RecoverNode {
    private String name;
    private String oldName;
    private String hostName;
    private String internalHostName;
    private Integer grpcPort;
    private Integer httpPort;
    private Integer internalGrpcPort;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOldName() {
        return oldName;
    }

    public void setOldName(String oldName) {
        this.oldName = oldName;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getInternalHostName() {
        return internalHostName;
    }

    public void setInternalHostName(String internalHostName) {
        this.internalHostName = internalHostName;
    }

    public Integer getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(Integer grpcPort) {
        this.grpcPort = grpcPort;
    }

    public Integer getInternalGrpcPort() {
        return internalGrpcPort;
    }

    public void setInternalGrpcPort(Integer internalGrpcPort) {
        this.internalGrpcPort = internalGrpcPort;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(Integer httpPort) {
        this.httpPort = httpPort;
    }
}
