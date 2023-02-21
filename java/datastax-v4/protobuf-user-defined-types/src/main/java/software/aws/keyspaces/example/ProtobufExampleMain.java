package software.aws.keyspaces.example;

/*-
 * #%L
 * AWS SigV4 Auth Java Driver 4.x Examples
 * %%
 * Copyright (C) 2020 - 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import software.aws.mcs.auth.SigV4AuthProvider;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;


/***
 *  The following is an example on how to use Protocol Buffers (Protobuf) as an alternative to User defined types.
 *  Protobuf is a free and open-source cross-platform data format used to serialize structured data. Storing the data as
 *  CQL BLOB to enable architects to deserialize structured values across applications and programming languages.
 */
public class ProtobufExampleMain {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        /***
         * The SigV4AuthProvider driver plugin enables you to add authentication information to your API requests
         * using the AWS Signature Version 4 Process (SigV4). Using the plugin, you can authorize users, roles,
         * and service roles access Amazon Keyspaces (for Apache Cassandra) using
         * AWS Identity and Access Management (IAM). */
        SigV4AuthProvider provider = new SigV4AuthProvider("us-east-1");

        /*** Provide the endpoint for Amazon Keyspaces. This can resolve as one or many endpoints.
         * Additional endpoints will be discovered from systsem.peers table .*/
        List<InetSocketAddress> contactPoints
            = Collections.singletonList(
            new InetSocketAddress("cassandra.us-east-1.amazonaws.com", 9142));

        /*** Creating session will auto-discover the application.conf file in the resources directory and load
         *  additional driver properties. */
        try (CqlSession session = CqlSession.builder()
            .addContactPoints(contactPoints)
            .withAuthProvider(provider)
            .withLocalDatacenter("us-east-1").build()) {

            /*** Keyspaces creates resources asynchronous these helpers will poll the system tables
             * creating a synchronous process */
            AmazonKeyspacesDataDefinitionLanguageHelpers.createKeyspaceSync(session, "aws");

            /*** The following table is created with CQL Blob which is used to
             * replace the User Defined type with protobuf */
            AmazonKeyspacesDataDefinitionLanguageHelpers.createTableSync(session, "aws", "protobuf_test",
                    "CREATE TABLE IF NOT EXISTS aws.protobuf_test (key text PRIMARY KEY, value text, data blob)");

            PreparedStatement preparedStatement
                    = session.prepare("INSERT INTO aws.protobuf_test (key, value, data) VALUES (?, ?, ?)");

            PreparedStatement readStatement
                    = session.prepare("SELECT * FROM aws.protobuf_test WHERE key = ?");


            /***
             * Imagine the following User Defined Types. phone_number consist of simple types
             * and user_profile contains complex types and nested types.
             *
             * CREATE TYPE aws.user_profile (
             *   id UUID,
             *   name text,
             *   age int,
             *   emails list<text>
             *   numbers map<text, frozen<phone_number>>
             * );
             *
             * CREATE TYPE aws.phone_number (
             *              *   country_code text,
             *              *   area_code text,
             *              *   prefix text,
             *              *   line text
             *              * );
             */

            /***
             * The following classes UserProfile and PhoneNumber are generated based on the UserProto.proto file.
             * The two classes match structure of the previously defined custom types.
             */
            UUID randomUUID = Uuids.random();

            System.out.println(randomUUID);
            UserProto.UserProfile proto = UserProto.UserProfile
                    .newBuilder()
                    .setId(convertUUIDtoProto(randomUUID))
                    .setName("Mike")
                    .setAge(100)
                    .addAllEmails(Arrays.
                            asList("mike@somesamplexyzdomain.com", "michael@somesamplexyzdomain.com"))
                    .putAllNumbers(Collections
                            .singletonMap("mobile", UserProto.PhoneNumber.newBuilder()
                                    .setCountryCode("1")
                                    .setAreaCode("718")
                                    .setPrefix("867")
                                    .setLine("5309").build())).build();


            /***
             * We then insert the protobuf object by converting it to a bytebuffer.
             */
            BoundStatement boundStatement = preparedStatement.boundStatementBuilder()
                    .setString("key", "mike")
                    .setString("value", "help")
                    .setBytesUnsafe("data", proto.toByteString().asReadOnlyByteBuffer())
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build();

            session.execute(boundStatement);

            /***
             * We then read the row back by selecting the partition key. xxwx
             */
            BoundStatement readbound = readStatement.boundStatementBuilder()
                    .setString("key", "mike")
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                    .build();

            ResultSet resultSet = session.execute(readbound);

            /***
             * When reading the row we parse it back into the java class from a byte buffer.
             */
            UserProto.UserProfile protoReturned = UserProto.UserProfile.parseFrom(resultSet.one().getByteBuffer("data"));

            System.out.println(convertProtoToUUID(protoReturned.getId()));
            System.out.print(protoReturned.toString());
        }

    }
    /*** The following helper will convert java UUID to protobuf representation ***/
    public static UserProto.Uuid convertUUIDtoProto(UUID value) {
        // The encoding format of our UUID is 16-bytes in big-endian byte order. Note: The initial byte
        // order of `ByteBuffer` is always big-endian.
        ByteBuffer bytes = ByteBuffer.allocate(16);
        bytes.putLong(0, value.getMostSignificantBits());
        bytes.putLong(8, value.getLeastSignificantBits());
        return UserProto.Uuid.newBuilder()
                .setValue(ByteString.copyFrom(bytes))
                .build();
    }
    /*** The following helper will convert protobuf representation to UUID ***/
    public static UUID convertProtoToUUID(UserProto.Uuid value){

        ByteString inBytes = value.getValue();

        if (inBytes.size() != 16) {
            throw new IllegalArgumentException(
                    "Expected 16 bytes for a uuid values, got " + inBytes.size());
        }
        ByteBuffer outBytes = ByteBuffer.allocate(16);
        inBytes.copyTo(outBytes);
        return new UUID(outBytes.getLong(0), outBytes.getLong(8));
    }
}
