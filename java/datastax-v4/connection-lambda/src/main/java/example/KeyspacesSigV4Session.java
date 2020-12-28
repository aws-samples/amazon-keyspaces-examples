/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: MIT-0
 */

package example;

import java.io.File;
import java.net.URL;
import javax.net.ssl.SSLContext;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.util.StringUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Represents a session open with Amazon Keyspaces using SigV4.
 * This is a singleton to help cut invocation time.
 *
 * @see <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html#java_tutorial.SigV4"/>
 * for instructions on setup of this auth type.
 */
public class KeyspacesSigV4Session
{
    private static final String SSL_TRUSTSTORE_PROPERTY_NAME = "javax.net.ssl.trustStore";
    private static final String SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME = "javax.net.ssl.trustStorePassword";
    private static final String SSL_TRUSTSTORE_FILENAME = "cassandra_truststore.jks";

    private static KeyspacesSigV4Session connection;
    private CqlSession session;

    public static KeyspacesSigV4Session Instance(LambdaLogger logger) throws Exception {
        if (connection == null) {
            logger.log("CREATING THE CASSANDRA SIGV4 CONNECTION.............");
            connection = new KeyspacesSigV4Session();
            connection.establishConnection(logger);
            logger.log("FINISHED THE CASSANDRA SIGV4 CONNECTION.............");
        }
        return connection;
    }

    private void establishConnection(LambdaLogger logger) throws Exception {
        if (session != null) {
            return;
        }

        File trustStore = getTrustStoreFile(SSL_TRUSTSTORE_FILENAME);

        System.setProperty(SSL_TRUSTSTORE_PROPERTY_NAME, trustStore.getAbsolutePath());
        String SSL_TRUSTSTORE_PASSWORD = System.getenv("SSL_TRUSTSTORE_PASSWORD");

        if (StringUtils.isNullOrEmpty(SSL_TRUSTSTORE_PASSWORD)) {
            logger.log("SSL_TRUSTSTORE_PASSWORD: not found");
        } else {
            logger.log("SSL_TRUSTSTORE_PASSWORD: found");
        }

        System.setProperty(SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME, SSL_TRUSTSTORE_PASSWORD);

        String region = System.getenv("AWS_REGION");
        logger.log("AWS_REGION: " + region);

        SSLContext context = SSLContext.getDefault();

        logger.log("Preparing to build session");
        DriverConfigLoader loader = DriverConfigLoader.fromClasspath("application.conf");

        this.session = CqlSession.builder()
                                 .withConfigLoader(loader)
                                 .build();
        logger.log("Finished building session");

    }
    private File getTrustStoreFile(String trustStoreFileName) {
        URL trustStoreResource = getClass().getClassLoader().getResource(trustStoreFileName);
        return new File(trustStoreResource.getFile());
    }

    public int validateConnection() {
        if (session == null)
            return 400;
        ResultSet rs = session.execute("select * from system.local;");
        Row row = rs.one();
        if (row != null)
            return 200;
        else
            return 400;
    }
}
