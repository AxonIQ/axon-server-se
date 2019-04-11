package io.axoniq.axonserver.migration;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Migration tool to map a AxonDB property file to an Axon Server property file.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class AxonDBPropertiesMigration {

    private static final Map<String, AxonServerProperty> propertyMap = new HashMap<>();

    static {
        propertyMap.put("axoniq.axondb.cluster.name", new AxonServerProperty("axoniq.axonserver.name"));
        propertyMap.put(
                "axoniq.axondb.cluster.internalport",
                new AxonServerProperty("axoniq.axonserver.internal-port", "8223")
        );
        propertyMap.put(
                "axoniq.axondb.cluster.internaldomain",
                new AxonServerProperty("axoniq.axonserver.internal-domain")
        );
        propertyMap.put("axoniq.axondb.port", new AxonServerProperty("axoniq.axonserver.port", "8123"));
        propertyMap.put(
                "axoniq.axondb.accesscontrol.cachettl",
                new AxonServerProperty("axoniq.axonserver.accesscontrol.cache-ttl")
        );
        propertyMap.put(
                "axoniq.axondb.accesscontrol.enabled",
                new AxonServerProperty("axoniq.axonserver.accesscontrol.enabled")
        );
        propertyMap.put(
                "axoniq.axondb.cluster.internaltoken",
                new AxonServerProperty("axoniq.axonserver.accesscontrol.internal-token"))
        ;
        propertyMap.put("axoniq.axondb.token", new AxonServerProperty("axoniq.axonserver.accesscontrol.token"));
        propertyMap.put("axoniq.axondb.domain", new AxonServerProperty("axoniq.axonserver.domain"));
        propertyMap.put("axoniq.axondb.keepalivetime", new AxonServerProperty("axoniq.axonserver.keep-alive-time"));
        propertyMap.put(
                "axoniq.axondb.minkeepalivetime",
                new AxonServerProperty("axoniq.axonserver.min-keep-alive-time"))
        ;
        propertyMap.put(
                "axoniq.axondb.keepalivetimeout",
                new AxonServerProperty("axoniq.axonserver.keep-alive-timeout")
        );
        propertyMap.put(
                "axoniq.axondb.ssl.certchainfile",
                new AxonServerProperty("axoniq.axonserver.ssl.cert-chain-file")
        );
        propertyMap.put(
                "axoniq.axondb.ssl.internalcertchainfile",
                new AxonServerProperty("axoniq.axonserver.ssl.internal-cert-chain-file")
        );
        propertyMap.put(
                "axoniq.axondb.ssl.privatekeyfile",
                new AxonServerProperty("axoniq.axonserver.ssl.private-key-file")
        );
        propertyMap.put("axoniq.axondb.ssl.enabled", new AxonServerProperty("axoniq.axonserver.ssl.enabled"));
        propertyMap.put(
                "axoniq.eventstore.synchronize.stream.initialnrofpermits",
                new AxonServerProperty("axoniq.axonserver.event-flow-control.initial-nr-of-permits")
        );
        propertyMap.put(
                "axoniq.eventstore.synchronize.stream.newpermitsthreshold",
                new AxonServerProperty("axoniq.axonserver.event-flow-control.new-permits-threshold")
        );
        propertyMap.put(
                "axoniq.eventstore.synchronize.stream.nrofnewpermits",
                new AxonServerProperty("axoniq.axonserver.event-flow-control.nr-of-new-permits")
        );
        propertyMap.put("axoniq.axondb.file.storage", new AxonServerProperty("axoniq.axonserver.event.storage"));
        propertyMap.put(
                "axoniq.axondb.file.bloomindexfpp",
                new AxonServerProperty("axoniq.axonserver.event.bloom-index-fpp")
        );
        propertyMap.put(
                "axoniq.axondb.file.maxsegmentsize",
                new AxonServerProperty("axoniq.axonserver.event.segment-size")
        );
        propertyMap.put(
                "axoniq.axondb.controldbpath",
                new AxonServerProperty(
                        "axoniq.axonserver.axondb.datasource", AxonDBPropertiesMigration::setAxonDBDatasource
                )
        );
        propertyMap.put(
                "axoniq.axondb.controldbbackuplocation",
                new AxonServerProperty("axoniq.axonserver.controldb-backup-location")
        );
        propertyMap.put("spring.datasource.url", new AxonServerProperty("axoniq.axonserver.axondb.datasource"));
        propertyMap.put("server.port", new AxonServerProperty("server.port", "8023"));
    }

    private static String setAxonDBDatasource(String controldbPath) {
        return String.format("jdbc:h2:nio:%s/axondb-controldb;DB_CLOSE_ON_EXIT=FALSE;DB_CLOSE_DELAY=-1",
                             controldbPath == null ? "." : controldbPath);
    }

    /**
     * Start the {@link AxonDBPropertiesMigration} which will map an AxonDB property file to an Axon Server property
     * file. The only property which is read from the {@code args} parameter is the first field, which specifies the
     * location of the AxonDB property file. If this is not provided, the file name is defaulted to
     * {@code axondb.properties}.
     *
     * @param args an Array of {@link String} providing the runtime arguments for this program
     * @throws IOException on any of the {@link File} operations which are performed to read the old property file and
     *                     write to the new one
     */
    public static void main(String[] args) throws IOException {
        String axondbFile = "axondb.properties";
        String axonserverFile = "axonserver.properties";

        if (args.length > 0) {
            axondbFile = args[0];
        }

        File axondbProperties = new File(axondbFile);
        if (!axondbProperties.exists() || !axondbProperties.canRead()) {
            System.err.println("Could not read AxonDB properties file: " + axondbProperties.getAbsolutePath());
            System.exit(-1);
        }
        File axonserverProperties = new File(axonserverFile);
        if (axonserverProperties.exists() && !axonserverProperties.canWrite()) {
            System.err.println("Could not write AxonServer properties file: " + axonserverProperties.getAbsolutePath());
            System.exit(-1);
        }

        Properties properties = new Properties();
        if (axondbFile.endsWith(".properties")) {
            properties.load(new FileInputStream(axondbFile));
        } else {
            Yaml yaml = new Yaml();
            Map map = yaml.load(new InputStreamReader(new FileInputStream(axondbProperties)));
            copyValues(map, properties, "");
        }

        Properties axonSeverProperties = new Properties();
        properties.forEach((key, value) -> {
            AxonServerProperty axonServerProperty = propertyMap.get(normalizeKey(String.valueOf(key)));
            if (axonServerProperty != null) {
                axonSeverProperties.setProperty(
                        axonServerProperty.getName(), axonServerProperty.legacyDefault(String.valueOf(value))
                );
            }
        });

        propertyMap.forEach((key, value) -> {
            if (value.hasLegacyDefault() && !axonSeverProperties.containsKey(value.getName())) {
                axonSeverProperties.setProperty(value.getName(), value.legacyDefault(null));
            }
        });

        axonSeverProperties.store(new FileWriter(axonserverProperties), null);
    }

    @SuppressWarnings("unchecked")
    private static void copyValues(Map map, Properties properties, String s) {
        map.forEach((key, value) -> {
            if (value instanceof Map) {
                copyValues(((Map) value), properties, s + key + ".");
            } else {
                properties.setProperty(s + key, String.valueOf(value));
            }
        });
    }

    private static String normalizeKey(String key) {
        return key.toLowerCase().replaceAll("-", "");
    }
}
