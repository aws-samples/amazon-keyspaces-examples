using System;
using System.Security.Cryptography.X509Certificates;
using Amazon.Lambda.Core;
using Cassandra;

namespace keyspaceslambdacsharp
{
    /*
     * Represents a session open with Amazon Keyspaces using Transport Layer Security (TLS).
     * The Singleton pattern enables re-using the open connection which helps with reducing the invocation time.
     */
    public class KeyspacesSession
    {
        private static KeyspacesSession connection;
        private Session session;

        public static KeyspacesSession Instance()
        {
            if (connection == null)
            {
                LambdaLogger.Log("Connecting to Keyspaces...");
                connection = new KeyspacesSession();
                connection.establishConnection();
                LambdaLogger.Log("Successfully established connection with Keyspaces.");
            }
            return connection;
        }

        private void establishConnection()
        {
            if (session != null)
                return;

            string region = Environment.GetEnvironmentVariable("AWS_REGION");
            string serviceUserName = Environment.GetEnvironmentVariable("SERVICE_USER_NAME");
            string servicePassword = Environment.GetEnvironmentVariable("SERVICE_PASSWORD");
            string certPath = Environment.GetEnvironmentVariable("CERTIFICATE_PATH");
            LambdaLogger.Log("Region: " + region + ", Username: " + serviceUserName + ", Certificate Path: " + certPath);

            X509Certificate2Collection certCollection = new X509Certificate2Collection();
            X509Certificate2 amazoncert = new X509Certificate2(@"" + certPath + "/AmazonRootCA1.pem");
            certCollection.Add(amazoncert);

            string awsEndpoint = "cassandra." + region + ".amazonaws.com";

            var cluster = Cluster.Builder()
                     .AddContactPoints(awsEndpoint)
                     .WithPort(9142)
                     .WithAuthProvider(new PlainTextAuthProvider(serviceUserName, servicePassword))
                     .WithSSL(new SSLOptions().SetCertificateCollection(certCollection))
                     .Build();

            session = (Session) cluster.Connect();
        }

        public int validateConnection()
        {
            if (session != null)
            {
                var rs = session.Execute("select * from system.local;");
                var rows = rs.GetRows();
                if (rows != null)
                    return 200;
            }
            LambdaLogger.Log("Something went wrong while establishing connection with Keyspaces. " +
                "Please check if the credentials are correct and the environement variables are correctly set.");
            return 400;
        }
    }
}
