package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

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
	contactPoint := fmt.Sprintf("cassandra.%s.amazonaws.com", awsRegion)
	fmt.Println("Using Contact Point ", contactPoint)

	// Configure Cluster
	cluster := gocql.NewCluster(contactPoint)

	cluster.Port = 9142

	awsAuth := sigv4.NewAwsAuthenticator()
	cluster.Authenticator = awsAuth

	//Retry Policy
	amazonKeyspacesRetry := &AmazonKeyspacesExponentialBackoffRetryPolicy{Max: 100, Min: 10, NumRetries: 10}
	cluster.RetryPolicy = amazonKeyspacesRetry

	// Configure Connection TrustStore for TLS
	cluster.SslOpts = &gocql.SslOptions{
		CaPath: "certs/sf-class2-root.crt",
		EnableHostVerification: false,
	}

	cluster.Consistency = gocql.LocalQuorum
	cluster.DisableInitialHostLookup = false

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
//AmazonKeyspacesExponentialBackoffRetryPolicy will retry exponentially on the same connection
type AmazonKeyspacesExponentialBackoffRetryPolicy struct {
	NumRetries int
	Min, Max   time.Duration
}

func (e *AmazonKeyspacesExponentialBackoffRetryPolicy) Attempt(q gocql.RetryableQuery) bool {

	if q.Attempts() > e.NumRetries {
		return false
	}
	time.Sleep(e.napTime(q.Attempts()))
	return true
}
// used to calculate exponentially growing time
func getExponentialTime(min time.Duration, max time.Duration, attempts int) time.Duration {
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	if max <= 0 {
		max = 10 * time.Second
	}
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1))
	// add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2)
	if napDuration > float64(max) {
		return time.Duration(max)
	}
	return time.Duration(napDuration)
}
// GetRetryType will always retry instead of RetryNextHost
func (e *AmazonKeyspacesExponentialBackoffRetryPolicy) GetRetryType(err error) gocql.RetryType {
	return gocql.Retry
}
func (e *AmazonKeyspacesExponentialBackoffRetryPolicy) napTime(attempts int) time.Duration {
	return getExponentialTime(e.Min, e.Max, attempts)
}
