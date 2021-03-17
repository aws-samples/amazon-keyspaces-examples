#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# 

require 'cassandra'

# load a variable with the Service Specific Credentials, to generate these see the below html.
# See https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.credentials.html
# directly)
login = ENV['SSC_USER_NAME']
password = ENV['SSC_PASSWORD']
region = ENV['AWS_REGION']

raise ("You must set the SSC_USER_NAME environment variable.") if  login.nil? || login.empty?
raise ("You must set the SSC_PASSWORD environment variable.") if  password.nil? || password.empty?
raise ("You must set the AWS_REGION environment variable.") if  region.nil? || region.empty?



cluster = Cassandra.cluster(
    username: login,
    password: password,
    ssl: true,
    # Get this by using curl...
    # $ curl https://certs.secureserver.net/repository/sf-class2-root.crt -O
    server_cert: './sf-class2-root.crt',
    hosts:  ["cassandra.#{region}.amazonaws.com"],
    port: 9142
)
session  = cluster.connect
puts "Connected successfully...."

future = session.execute_async('SELECT keyspace_name, table_name FROM system_schema.tables')
future.on_success do |rows|
  rows.each do |row|
    puts "Found keyspace: #{row['keyspace_name']} table: #{row['table_name']}"
  end
end
future.join
