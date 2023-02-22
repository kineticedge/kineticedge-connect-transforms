package io.kineticedge.kafka.connect.transforms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Pull the version in from the build process and make it available within the connector.
 *
 */
public class Version {

    private static final Logger log = LoggerFactory.getLogger(Version.class);

    private static final String VERSION_FILE = "/version.properties";

    private static final String version;

    static {

        String value = "unknown";

        try {
            Properties props = new Properties();
            try (InputStream versionFileStream = Version.class.getResourceAsStream(VERSION_FILE)) {
                props.load(versionFileStream);
                value = props.getProperty("version", value).trim();
            }
        } catch (Exception e) {
            value = "unknown";
            log.warn("Error while loading version, version set to 'unknown'.", e);
        }

        version = value;
    }

    public static String getVersion() {
        return version;
    }
}