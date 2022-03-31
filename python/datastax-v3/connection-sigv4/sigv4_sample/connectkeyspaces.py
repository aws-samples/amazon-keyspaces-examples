#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
try:
    import os
    import boto3
    import ssl
    import sys
    from boto3 import Session
    from cassandra_sigv4.auth import AuthProvider, Authenticator, SigV4AuthProvider
    from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra import ConsistencyLevel
    from cassandra.query import SimpleStatement
    from KeyspacesRetryPolicy import KeyspacesRetryPolicy

except ImportError:
    raise RuntimeError('Required packages Failed To install please run "python Setup.py install" command or install '
                       'using pip')

def main():
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        cert_path = os.path.join(os.path.dirname(__file__), 'resources/sf-class2-root.crt')
        ssl_context.load_verify_locations(cert_path)
        ssl_context.verify_mode = CERT_REQUIRED
        
        # this will automatically pull the credentials from either the
        # ~/.aws/credentials file
        # ~/.aws/config 
        # or from the boto environment variables.
        boto_session = boto3.Session()
        
        # verify that the session is set correctly
        credentials = boto_session.get_credentials()
        
        if not credentials or not credentials.access_key:
            sys.exit("No access key found, please setup credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) according to https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence\n")
    
        region = boto_session.region_name

        if not region:  
            sys.exit("You do not have a region set.  Set environment variable AWS_REGION or provide a configuration see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence\n")
            

        auth_provider = SigV4AuthProvider(boto_session)
        contact_point = "cassandra.{}.amazonaws.com".format(region)

        profile = ExecutionProfile(
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            retry_policy=KeyspacesRetryPolicy(RETRY_MAX_ATTEMPTS=5))

        cluster = Cluster([contact_point], 
                         ssl_context=ssl_context, 
                         auth_provider=auth_provider,
                         port=9142,
                         execution_profiles={EXEC_PROFILE_DEFAULT: profile})

        session = cluster.connect()
        print("CONNECTION TO KEYSPACES SUCCESSFUL2")
       
        rows = session.execute('select * from system_schema.keyspaces')
        print("PRINTING SCHEMA INFORMATION2")
        for r in rows.current_rows:  
            print("Found Keyspace: {}".format(r.keyspace_name))

if __name__ == '__main__':
    main()

