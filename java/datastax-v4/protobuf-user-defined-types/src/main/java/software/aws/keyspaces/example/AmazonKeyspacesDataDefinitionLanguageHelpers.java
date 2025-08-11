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
/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AmazonKeyspacesDataDefinitionLanguageHelpers {

    private static String KEYSPACE_SIMPLE_STRATEGY = "{'class': 'SimpleStrategy', 'replication_factor' : 3}";

    private static long KEYSPACE_MIN_WAIT_TIME_IN_MS = 25;

    private static long DEFAULT_WAIT = 60 * 1000;

    private static TimeUnit DEFAULT_TIMEUNIT = TimeUnit.MILLISECONDS;

    public static Boolean createKeyspaceSync(CqlSession session, String keyspaceName) {
        return createKeyspaceSync(session, keyspaceName, null, KEYSPACE_SIMPLE_STRATEGY);
    }
    public static Boolean createKeyspaceSync(CqlSession session, String keyspaceName, Map<String, String> tags) {
        return createKeyspaceSync(session, keyspaceName, tags, KEYSPACE_SIMPLE_STRATEGY);
    }

    public static Boolean createKeyspaceSync(CqlSession session, String keyspaceName, Map<String, String> tags, String replicationStrategy) {
        return createKeyspaceSync(session, keyspaceName, tags, replicationStrategy, DEFAULT_WAIT, DEFAULT_TIMEUNIT);
    }

    public static Boolean createKeyspaceSync(CqlSession session, String keyspaceName, Map<String, String> tags, String replicationStrategy, long totalWait, TimeUnit timeUnit) {

        StringBuilder keyspaceStatement = new StringBuilder()
                .append("CREATE KEYSPACE IF NOT EXISTS ")
                .append(keyspaceName)
                .append(" WITH REPLICATION = ")
                .append(replicationStrategy);

        if (tags != null) {
            keyspaceStatement.append(" AND TAGS = {");

            String tagString = tags.entrySet().stream().map(e -> String.format("'%s' : '%s'", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(", "));

            keyspaceStatement.append(tagString);

            keyspaceStatement.append("}");
        }
        return createKeyspaceSync(session, keyspaceName, keyspaceStatement.toString(), totalWait, timeUnit);
    }

    public static Boolean createKeyspaceSync(CqlSession session, String keyspaceName, String createKeyspaceStatement, long totalWait, TimeUnit timeUnit) {

        session.execute(createKeyspaceStatement);

        boolean keyspaceExist = doesKeyspaceExist(session, keyspaceName);

        Stopwatch sw = Stopwatch.createStarted();

        while (keyspaceExist != true && sw.elapsed(timeUnit) < totalWait) {
            Uninterruptibles.sleepUninterruptibly(KEYSPACE_MIN_WAIT_TIME_IN_MS, TimeUnit.MILLISECONDS);

            keyspaceExist = doesKeyspaceExist(session, keyspaceName);
        }
        return keyspaceExist;
    }

    public static boolean doesKeyspaceExist(CqlSession session, String keyspaceName) {

        //When creating a Keyspace with Capital letters Cassandra will perform 'tolower'  if not surrounded by quotes
        //Querying with the capital letter results in 0 results
        //Here we perform to lower when appropriate before querying for keyspace name
        String keyspaceToQuery = (keyspaceName.matches("^[^\"]*[A-Z]+[^\"]*$"))
                ? keyspaceName.toLowerCase() : keyspaceName;

        return session.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspaceToQuery)
                .all()
                .stream().count() > 0 ? true : false;
    }

    public static Boolean createTableSync(CqlSession session, String keyspaceName, String tableName, String createTableStatement) {
        return createTableSync(session, keyspaceName, tableName, createTableStatement, DEFAULT_WAIT, DEFAULT_TIMEUNIT);
    }
    public static Boolean createTableSync(CqlSession session, String keyspaceName, String tableName, String createTableStatement, long totalWait, TimeUnit timeUnit) {

        //execute create table statement
        session.execute(createTableStatement);

        Stopwatch sw = Stopwatch.createStarted();

        boolean tableIsActive = isTableActive(session, keyspaceName, tableName);

        while (tableIsActive != true && sw.elapsed(timeUnit) < totalWait) {
            Uninterruptibles.sleepUninterruptibly(KEYSPACE_MIN_WAIT_TIME_IN_MS, TimeUnit.MILLISECONDS);

            tableIsActive = isTableActive(session, keyspaceName, tableName);
        }
        System.out.println("is table active? :" + tableIsActive);
        return tableIsActive;
    }

    public static boolean isTableActive(CqlSession session, String keyspaceName, String tableName) {

        //When creating a Table with Capital letters Cassandra will perform 'tolower'  if not surrounded by quotes
        //Querying with the capital letter results in 0 results
        //Here we perform to lower when appropriate before querying for keyspace name
        String keyspaceToQuery = (keyspaceName.matches("^[^\"]*[A-Z]+[^\"]*$"))
                ? keyspaceName.toLowerCase() : keyspaceName;

        String tableToQuery = (tableName.matches("^[^\"]*[A-Z]+[^\"]*$"))
                ? tableName.toLowerCase() : tableName;

        return session.execute("SELECT * FROM system_schema_mcs.tables WHERE keyspace_name = ? and table_name = ?", keyspaceToQuery, tableToQuery)
                .all()
                .stream().anyMatch(x ->( x.getString("status").equals("ACTIVE")));
    }
}
