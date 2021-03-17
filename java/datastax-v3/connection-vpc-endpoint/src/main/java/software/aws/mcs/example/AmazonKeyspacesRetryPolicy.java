package software.aws.mcs.example;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

/****
 * The Amazon Keyspaces Retry Policy is an alternative to the DefaultRetryPolicy for the Cassandra 3.x Driver.
 *
 * The main difference between the DefaultRetryPolicy and the AmazonKeyspacesRetryPolicy is that
 * the AmazonKeyspacesRetryPolicy will retry request a configuable number of times. By default we take a conservative
 * approach of 3 retry attempts. Additionally, this policy will not retry on the nexthost which can result in
 * NoHostAvailableExceptions
 *
 */


public class AmazonKeyspacesRetryPolicy implements RetryPolicy {

    final int maxNumberOfRetries;

    public AmazonKeyspacesRetryPolicy() {
        maxNumberOfRetries = 3;
    }

    public AmazonKeyspacesRetryPolicy(int numberOfRetries) {
        this.maxNumberOfRetries = numberOfRetries;
    }

    protected RetryDecision makeDecisionBasedOnNumberOfConfiguredRetries(int nbRetry, ConsistencyLevel cl){
        if(nbRetry > maxNumberOfRetries){
            return RetryDecision.rethrow();
        }

        return RetryDecision.retry(cl);
    }

    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        return  makeDecisionBasedOnNumberOfConfiguredRetries(nbRetry, cl);
    }

    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        return  makeDecisionBasedOnNumberOfConfiguredRetries(nbRetry, cl);
    }

    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return  makeDecisionBasedOnNumberOfConfiguredRetries(nbRetry, (ConsistencyLevel)null);
    }

    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        return  makeDecisionBasedOnNumberOfConfiguredRetries(nbRetry, cl);
    }

    public void init(Cluster cluster) {
    }

    public void close() {
    }


}
