package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sigv4-auth-cassandra-gocql-driver-plugin/sigv4"
	"github.com/gocql/gocql"
)

var awsSession *session.Session
var awsRegion string

type CassandraMeta struct {
	Keyspace string
	Table    string
}

func init() {
	//Establish AWS Session
	awsSession = session.New()
	awsRegion = os.Getenv("AWS_REGION")
	if len(awsRegion) == 0 {
		//Fallback to DEFAULT_AWS_REGION if AWS_REGION missing
		awsRegion = os.Getenv("AWS_DEFAULT_REGION")
		if len(awsRegion) == 0 {
			fmt.Println("No AWS_REGION or AWS_DEFAULT_REGION env variable specified")
			os.Exit(-1)
		}
	}
}

func main() {

	//Determine Contact Point
	contactPoint := fmt.Sprintf("cassandra.%s.amazonaws.com:9142", awsRegion)
	fmt.Println("Using Contact Point ", contactPoint)

	// Configure Cluster
	cluster := gocql.NewCluster(contactPoint)

	awsAuth := sigv4.NewAwsAuthenticator()
	cluster.Authenticator = awsAuth

	// Configure Connection TrustStore for TLS
	cluster.SslOpts = &gocql.SslOptions{
		CaPath: "certs/sf-class2-root.crt",
	}

	cluster.Consistency = gocql.LocalQuorum
	cluster.DisableInitialHostLookup = true

	cassandraSession, err := cluster.CreateSession()
	if err != nil {
		fmt.Println("Cassandra Session Creation Error - ", err)
		os.Exit(-2)
	}

	defer cassandraSession.Close()

	// Perform Query
	var keyspaceName string
	iter := cassandraSession.Query("SELECT keyspace_name FROM system_schema.keyspaces;").Iter()
	defer iter.Close()

	for iter.Scan(&keyspaceName) {
		fmt.Println("keyspace_name : ", keyspaceName)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}
