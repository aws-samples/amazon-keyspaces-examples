/**
 * The default implementation triggers a retry on the next host in the query plan with the same consistency level, for historical reasons.
 * 
 * This is a conservative retry policy adapted for the Amazon Keyspaces Service.
 * It allows for a configurable number of attempts, but by default the number of attempts is {@value max_retry_count#default_retry_count}
 * <p>
 * This policy will either reattempt request on the same host or rethrow the exception to the calling thread. The main difference between
 * this policy from the original {@link cassandra.policies.retry.RetryPolicy} is that the {@link AmazonKeyspacesRetryPolicy} will call {@link useCurrentHost} 
 * <p>
 * In Amazon Keyspaces, it's likely that {@link WriteTimeoutException} or {@link ReadTimeoutException} is the result of exceeding current table
 * capacity. Learn more about Amazon Keyspaces capacity here: @see <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/ReadWriteCapacityMode.html">Amazon Keyspaces CapacityModes</a>.
 * In most cases you should allow for small number of retries, and handle the exception in your application threads.
 *
 * <p>To activate this policy, modify the {@code client connection} section in the driver
 * configuration, for example:
 *
const client = new cassandra.Client({
                   contactPoints: ['cassandra.us-east-2.amazonaws.com'],
                   localDataCenter: 'us-east-2',
                   policies: { retry: new retry.AmazonKeyspacesRetryPolicy(Max_retry_attempts) },
                   pooling: { coreConnectionsPerHost: { [types.distance.local]: 2 } },
                   queryOptions: { isIdempotent: true, consistency: cassandra.types.consistencies.localQuorum },
                   authProvider: auth,
                   sslOptions: sslOptions1,
                   protocolOptions: { port: 9142 }
        });

 * </pre>
 */


'use strict';
const util = require('util');
const cassandra = require('cassandra-driver');
const default_retry_count = 3 // default number of retries on same host
const RetryPolicy = cassandra.policies.retry.RetryPolicy
        
/** @module policies/retry */
/**
* Custom AmazonKeyspacesRetryPolicy.
* Determines what to do when the drivers runs into an specific Cassandra exception
* @constructor
*/
function AmazonKeyspacesRetryPolicy( max_retry_count ) {
    if (max_retry_count) {
                this.retry_count = max_retry_count;
        }
    else{
                this.retry_count = default_retry_count;
        }
}
        
util.inherits (AmazonKeyspacesRetryPolicy, RetryPolicy);
        
/**
* Determines what to do when the driver gets an UnavailableException response
* @param {OperationInfo} info
* @param {Number} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
* the exception.
* @param {Number} required The number of replicas whose response is required to achieve the
* required [consistency]{@link module:types~consistencies}.
* @param {Number} alive The number of replicas that were known to be alive when the request had been processed
* (since an unavailable exception has been triggered, there will be alive &lt; required)
* @returns {DecisionInfo}
*/
AmazonKeyspacesRetryPolicy.prototype.onUnavailable = function (info, consistency, required, alive) {
    if (info.nbRetry > this.retry_count) {
                return this.rethrowResult();
        }

        return this.retryResult(consistency, true);
};
        
/**
* Determines what to do when the driver gets a ReadTimeoutException response 
* @param {OperationInfo} info
* @param {Number} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
* the exception.
* @param {Number} received The number of nodes having answered the request.
* @param {Number} blockFor The number of replicas whose response is required to achieve the
* required [consistency]{@link module:types~consistencies}.
* @param {Boolean} isDataPresent When <code>false</code>, it means the replica that was asked for data has not responded.
* @returns {DecisionInfo}
*/
AmazonKeyspacesRetryPolicy.prototype.onReadTimeout = function (info, consistency, received, blockFor, isDataPresent) {
    if (info.nbRetry > this.retry_count) {
        return this.rethrowResult();
    }
        return this.retryResult(consistency, true);
};
        
/**
* Determines what to do when the driver gets a WriteTimeoutException 
* @param {OperationInfo} info
* @param {Number} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
* the exception.
* @param {Number} received The number of nodes having acknowledged the request.
* @param {Number} blockFor The number of replicas whose acknowledgement is required to achieve the required
* [consistency]{@link module:types~consistencies}.
* @param {String} writeType A <code>string</code> that describes the type of the write that timed out ("SIMPLE"
* / "BATCH" / "BATCH_LOG" / "UNLOGGED_BATCH" / "COUNTER").
* @returns {DecisionInfo}
*/
AmazonKeyspacesRetryPolicy.prototype.onWriteTimeout = function (info, consistency, received, blockFor, writeType) {
    if (info.nbRetry > this.retry_count) {
        return this.rethrowResult();
    }
    return this.retryResult(consistency, true);
};
        
/**
* Defines whether to retry and at which consistency level on an unexpected error.
* <p>
* @param {OperationInfo} info
* @param {Number|undefined} consistency The [consistency]{@link module:types~consistencies} level of the query that triggered
* the exception.
* @param {Error} err The error that caused this request to fail.
* @returns {DecisionInfo}
*/
AmazonKeyspacesRetryPolicy.prototype.onRequestError = function (info, consistency, err) {
    if (info.nbRetry > this.retry_count) {
        return this.rethrowResult();
    }
    return this.retryResult(consistency, true);
};
        
/**
* Returns a {@link DecisionInfo} to retry the request with the given [consistency]{@link module:types~consistencies}.
* @param {Number|undefined} [consistency] When specified, it retries the request with the given consistency.
* @param {Boolean} [useCurrentHost] When specified, determines if the retry should be made using the same host.
* Default: true.
* @returns {DecisionInfo}
*/
AmazonKeyspacesRetryPolicy.prototype.retryResult = function (consistency, useCurrentHost) {
    return {
            decision: RetryPolicy.retryDecision.retry,
            consistency: consistency,
            useCurrentHost: useCurrentHost !== false
        };
};
        
/**
* Returns a {@link DecisionInfo} to callback in error when a err is obtained for a given request.
* @returns {DecisionInfo}
*/
AmazonKeyspacesRetryPolicy.prototype.rethrowResult = function () {
          return { decision: AmazonKeyspacesRetryPolicy.retryDecision.rethrow };
};
        
/**
* Determines the retry decision for the retry policies.
* @type {Object}
* @property {Number} rethrow
* @property {Number} retry
* @property {Number} ignore
* @static
*/
AmazonKeyspacesRetryPolicy.retryDecision = {
    rethrow:  0,
    retry:    1,
    ignore:   2
};
        
exports.AmazonKeyspacesRetryPolicy = AmazonKeyspacesRetryPolicy;