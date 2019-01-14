package io.axoniq.axonserver.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

import java.net.UnknownHostException;

/**
 * @author Marc Gathier
 */
@Configuration
@ConfigurationProperties(prefix = "axoniq.axonserver")
public class MessagingPlatformConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(MessagingPlatformConfiguration.class);
    /**
     * gRPC port for axonserver platform
     */
    private int port = 8000;
    /**
     * gRPC port for communication between messing platform nodes
     */
    private int internalPort = 8001;
    /**
     * Node name of this axonserver platform node, if not set defaults to the hostname
     */
    private String name;
    /**
     * Hostname of this node as communicated to clients, defaults to the result of hostname command
     */
    private String hostname;
    /**
     * Domain of this node as communicated to clients. Optional, if set will be appended to the hostname in communication
     * with clients.
     */
    private String domain;
    /**
     * Hostname as communicated to other nodes of the cluster. Defaults to hostname.
     */
    private String internalHostname;
    /**
     * Domain as communicated to other nodes of the cluster. Optional, if not set, it will use the domain value.
     */
    private String internalDomain;

    /**
     * Internal, used to cache the value for the HTTP port.
     */
    private int httpPort;

    private long keepAliveTimeout = 5000;
    private long keepAliveTime = 0;
    private long minKeepAliveTime = 1000;



    @NestedConfigurationProperty
    private SslConfiguration ssl = new SslConfiguration();
    @NestedConfigurationProperty
    private AccessControlConfiguration accesscontrol = new AccessControlConfiguration();

    @NestedConfigurationProperty
    private FlowControl commandFlowControl = new FlowControl();

    @NestedConfigurationProperty
    private FlowControl queryFlowControl = new FlowControl();

    @NestedConfigurationProperty
    private ClusterConfiguration cluster = new ClusterConfiguration();

    /**
     * Rate for synchronization of metrics information between nodes
     */
    private int metricsSynchronizationRate;

    private int metricsInterval =15;

    /**
     * The contexts that this hub node will be connected to. Valid values are
     * <tt>all-contexts</tt>, <tt>node-contexts</tt> and <tt>default-context</tt>.
     */
    private String connect2contexts = "node-contexts";

    private final SystemInfoProvider systemInfoProvider;
    private int internalExecutorThreads;
    private int executorThreads ;
    private int bossThreads;
    private int workerThreads;
    private String controldbBackupLocation = ".";
    private int maxMessageSize = 0;

    public MessagingPlatformConfiguration(SystemInfoProvider systemInfoProvider) {
        this.systemInfoProvider = systemInfoProvider;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getInternalPort() {
        return internalPort;
    }

    public void setInternalPort(int internalPort) {
        this.internalPort = internalPort;
    }

    public String getName() {
        if( name == null) {
            name = getHostname();
        }

        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHostname() {
        if( hostname == null) {
            try {
                hostname = systemInfoProvider.getHostName();
                if( domain != null && hostname.endsWith("." + domain)) {
                        hostname = hostname.substring(0, hostname.length() - domain.length() - 1);
                }
            } catch (UnknownHostException e) {
                logger.warn("Could not determine hostname from inet address: {}", e.getMessage());
            }
        }
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getInternalHostname() {
        if( internalHostname == null) {
            internalHostname = getHostname();
        }
        return internalHostname;
    }

    public void setInternalHostname(String internalHostname) {
        this.internalHostname = internalHostname;
    }

    public String getInternalDomain() {
        if( internalDomain == null) {
            internalDomain = getDomain();
        }
        return internalDomain;
    }

    public int getHttpPort() {
        if( httpPort == 0) {
            httpPort = systemInfoProvider.getPort();

        }
        return httpPort;
    }

    public void setInternalDomain(String internalDomain) {
        this.internalDomain = internalDomain;
    }

    public String getFullyQualifiedHostname() {
        if( getDomain() != null ) return getHostname() + "." + getDomain();

        return getHostname();
    }

    public String getFullyQualifiedInternalHostname() {
        if( getInternalDomain() != null) return getInternalHostname() + "." + getInternalDomain();

        return getInternalHostname();
    }

    public SslConfiguration getSsl() {
        return ssl;
    }

    public void setSsl(SslConfiguration ssl) {
        this.ssl = ssl;
    }

    public AccessControlConfiguration getAccesscontrol() {
        return accesscontrol;
    }

    public void setAccesscontrol(AccessControlConfiguration accesscontrol) {
        this.accesscontrol = accesscontrol;
    }

    public FlowControl getCommandFlowControl() {
        return commandFlowControl;
    }

    public void setCommandFlowControl(FlowControl commandFlowControl) {
        this.commandFlowControl = commandFlowControl;
    }

    public FlowControl getQueryFlowControl() {
        return queryFlowControl;
    }

    public void setQueryFlowControl(FlowControl queryFlowControl) {
        this.queryFlowControl = queryFlowControl;
    }

    public ClusterConfiguration getCluster() {
        return cluster;
    }

    public void setCluster(ClusterConfiguration cluster) {
        this.cluster = cluster;
    }

    public int getMetricsInterval() {
        return metricsInterval;
    }

    public void setMetricsInterval(int metricsInterval) {
        this.metricsInterval = metricsInterval;
    }

    public String getConnect2contexts() {
        return connect2contexts;
    }

    public void setConnect2contexts(String connect2contexts) {
        this.connect2contexts = connect2contexts;
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

    public long getMinKeepAliveTime() {
        return minKeepAliveTime;
    }

    public void setMinKeepAliveTime(long minKeepAliveTime) {
        this.minKeepAliveTime = minKeepAliveTime;
    }

    public int getInternalExecutorThreads() {
        return internalExecutorThreads;
    }

    public void setInternalExecutorThreads(int internalExecutorThreads) {
        this.internalExecutorThreads = internalExecutorThreads;
    }

    public int getExecutorThreads() {
        return executorThreads;
    }

    public void setExecutorThreads(int executorThreads) {
        this.executorThreads = executorThreads;
    }

    public int getBossThreads() {
        return bossThreads;
    }

    public void setBossThreads(int bossThreads) {
        this.bossThreads = bossThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public String getControldbBackupLocation() {
        return controldbBackupLocation;
    }

    public void setControldbBackupLocation(String controldbBackupLocation) {
        this.controldbBackupLocation = controldbBackupLocation;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

}
