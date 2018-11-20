package io.axoniq.axonserver.cluster;

public class RaftServerConfiguration {
    private int internalPort;
    private long minKeepAliveTime;
    private int maxMessageSize;
    private long keepAliveTime;
    private long keepAliveTimeout;

    public int getInternalPort() {
        return internalPort;
    }

    public void setInternalPort(int internalPort) {
        this.internalPort = internalPort;
    }

    public long getMinKeepAliveTime() {
        return minKeepAliveTime;
    }

    public void setMinKeepAliveTime(long minKeepAliveTime) {
        this.minKeepAliveTime = minKeepAliveTime;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public long getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public void setKeepAliveTimeout(long keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
    }
}
