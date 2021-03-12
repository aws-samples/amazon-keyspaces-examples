package software.aws.mcs.example;

/*-
 * #%L
 * AWS SigV4 Auth Java Driver 4.x Examples
 * %%
 * Copyright (C) 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * #L%
 */

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import software.aws.mcs.auth.SigV4AuthProvider;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public class OrderFetcher {
    public final static String TABLE_FORMAT = "%-25s%s\n";
    public final static int KEYSPACES_PORT = 9142;

    public static Cluster connectToCluster(String region, List<InetSocketAddress> contactPoints) {
        // VPC endpoints appear as a single, infinitely scalable and highly available node. To optimize for throughput
        // and availability we recommend increasing the number of local connections to at least 9. For more info
        // on throughput tuning, see https://docs.aws.amazon.com/keyspaces/latest/devguide/functional-differences.html#functional-differences.query-throughput-tuning
        PoolingOptions poolingOptions = new PoolingOptions(); // [1]
        poolingOptions
                .setConnectionsPerHost(HostDistance.LOCAL,  9 /* min connections */, 9 /* max connections */);

        // The default retry policy in the driver retries the next node. However, as VPC endpoints appear as single nodes,
        // the default retry policy essentially has no retries. To re-enable retries, we recommend using a retry policy
        // that defaults the retry same node. Retries are helpful for handling scaling scenarios where an application
        // may have exceeded the table capacity ahead of Keyspaces scaling up to accommodate the traffic. For more
        // on serverless resource management, see https://docs.aws.amazon.com/keyspaces/latest/devguide/ReadWriteCapacityMode.html
        AmazonKeyspacesRetryPolicy retryPolicy = new AmazonKeyspacesRetryPolicy();

        SigV4AuthProvider provider = new SigV4AuthProvider(region);
        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(contactPoints)
                .withPort(KEYSPACES_PORT)
                .withAuthProvider(provider)
                .withSSL()
                .withPoolingOptions(poolingOptions)
                .withRetryPolicy(retryPolicy)
                .build();

        // To enable retries for writes, enable query idempotence. This can also be done per request, if only some writes
        // idempotent.
        cluster.getConfiguration().getQueryOptions().setDefaultIdempotence(true);

        return cluster;
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: OrderFetcher <region> <endpoint> <customer ID>");
            System.exit(1);
        }

        String region = args[0];
        List<InetSocketAddress> contactPoints = Collections.singletonList(new InetSocketAddress(args[1], KEYSPACES_PORT));

        try (Cluster cluster = connectToCluster(region, contactPoints)) {
            Session session = cluster.connect();

            // Use a prepared query for quoting
            PreparedStatement prepared = session.prepare("select * from acme.orders where customer_id = ?");

            // We use execute to send a query to Cassandra. This returns a ResultSet, which is essentially a collection
            // of Row objects.
            ResultSet rs = session.execute(prepared.bind(args[2]));

            // Print the header
            System.out.printf(TABLE_FORMAT, "Date", "Order Id");

            for (Row row : rs) {
                System.out.printf(TABLE_FORMAT, row.getTimestamp("order_timestamp"), row.getUUID("order_id"));
            }
        }

    }
}
