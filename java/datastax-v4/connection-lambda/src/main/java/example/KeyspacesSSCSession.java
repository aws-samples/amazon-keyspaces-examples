/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: MIT-0
 */

package example;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import javax.net.ssl.SSLContext;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.util.StringUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Represents a session open with Amazon Keyspaces using Server side credentials (SSC).
 * This is a singleton to help cut invocation time.
 *
 * @see <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html#java_tutorial"/>
 * for instructions on setup of this auth type.
 */
public class KeyspacesSSCSession
{
    private static final String SSL_TRUSTSTORE_PROPERTY_NAME = "javax.net.ssl.trustStore";
    private static final String SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME = "javax.net.ssl.trustStorePassword";
    private static final String SSL_TRUSTSTORE_FILENAME = "cassandra_truststore.jks";

    private CqlSession session;

    private static KeyspacesSSCSession connection;

    public static KeyspacesSSCSession Instance(LambdaLogger logger) throws Exception {
        if (connection == null) {
            logger.log("CREATING THE CASSANDRA SSC CONNECTION.............");
            connection = new KeyspacesSSCSession();
            connection.establishConnection(logger);
            logger.log("FINISHED THE CASSANDRA SSC CONNECTION.............");
        }
        return connection;
    }

    private void establishConnection(LambdaLogger logger) throws Exception {
        if (session != null) {
            return;
        }

        logger.log("AWS_ACCESS_ID: " + System.getenv("AWS_ACCESS_KEY_ID"));
        String region = System.getenv("AWS_REGION");
        logger.log("AWS_REGION: " + region);

        String sscUserName = System.getenv("SSC_USER_NAME");
        logger.log("SSC USER NAME: " + sscUserName);

        String sscUserPassword= System.getenv("SSC_PASSWORD");
        if (StringUtils.isNullOrEmpty(sscUserName)) {
            logger.log("SSC Password: not found");
        } else {
            logger.log("SSC Password: found");
        }

        String SSL_TRUSTSTORE_PASSWORD= System.getenv("SSL_TRUSTSTORE_PASSWORD");
        if (StringUtils.isNullOrEmpty(SSL_TRUSTSTORE_PASSWORD)) {
            logger.log("SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME: not found");
        } else {
            logger.log("SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME: found");
        }

        File trustStore = getTrustStoreFile(SSL_TRUSTSTORE_FILENAME);


        System.setProperty(SSL_TRUSTSTORE_PROPERTY_NAME, trustStore.getAbsolutePath());
        System.setProperty(SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME, SSL_TRUSTSTORE_PASSWORD);

        SSLContext context = SSLContext.getDefault();
        logger.log("Preparing to build session");

        String contactPoint = "cassandra." + region + ".amazonaws.com";
        this.session = CqlSession.builder()
                                 .withSslContext(context)
                                 .withAuthCredentials(sscUserName,sscUserPassword)
                                 .addContactPoint(new InetSocketAddress(contactPoint, 9142))
                                 .withLocalDatacenter(region)
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
