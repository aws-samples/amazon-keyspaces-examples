package com.aws.ssa.keyspaces.retry;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a conservative retry policy adapted for the Amazon Keyspaces Service.
 * It allows for a configurable number of attempts, but by default the number of attempts is {@value AmazonKeyspacesRetryOption#DEFAULT_KEYSPACES_RETRY_MAX_ATTEMPTS}
 * <p>
 * This policy will either reattempt request on the same host or rethrow the exception to the calling thread. The main difference between
 * this policy from the original {@link com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy} is that the {@link AmazonKeyspacesRetryPolicy} will call {@link RetryDecision#RETRY_SAME} instead of {@link RetryDecision#RETRY_NEXT}
 * <p>
 * In Amazon Keyspaces, it's likely that {@link WriteTimeoutException} or {@link ReadTimeoutException} is the result of exceeding current table
 * capacity. Learn more about Amazon Keyspaces capacity here: @see <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/ReadWriteCapacityMode.html">Amazon Keyspaces CapacityModes</a>.
 * In most cases you should allow for small number of retries, and handle the exception in your application threads.
 *
 * <p>To activate this policy, modify the {@code advanced.retry-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.retry-policy {
 *     class = com.aws.ssa.keyspaces.retry.AmazonKeyspacesRetryPolicy
 *     max-attempts = 2
 *   }
 * }
 * </pre>
 */

@ThreadSafe
public class AmazonKeyspacesRetryPolicy implements RetryPolicy {


    private static final Logger LOG = LoggerFactory.getLogger(AmazonKeyspacesRetryPolicy.class);
    @VisibleForTesting
    public static final String RETRYING_ON_READ_TIMEOUT = "[{}] Retrying on read timeout on same host (consistency: {}, required responses: {}, received responses: {}, data retrieved: {}, retries: {})";
    @VisibleForTesting
    public static final String RETRYING_ON_WRITE_TIMEOUT = "[{}] Retrying on write timeout on same host (consistency: {}, write type: {}, required acknowledgments: {}, received acknowledgments: {}, retries: {})";
    @VisibleForTesting
    public static final String RETRYING_ON_UNAVAILABLE = "[{}] Retrying on unavailable exception on next host (consistency: {}, required replica: {}, alive replica: {}, retries: {})";
    @VisibleForTesting
    public static final String RETRYING_ON_ABORTED = "[{}] Retrying on aborted request on next host (retries: {})";
    @VisibleForTesting
    public static final String RETRYING_ON_ERROR = "[{}] Retrying on node error on next host (retries: {})";

    private final String logPrefix;

    private final Integer maxRetryCount;


    public AmazonKeyspacesRetryPolicy(DriverContext context) {
        this(context, context.getConfig().getDefaultProfile().getName());
    }

    public AmazonKeyspacesRetryPolicy(DriverContext context, Integer maxRetryCount) {

        String profileName = context.getConfig().getDefaultProfile().getName();

        this.maxRetryCount = maxRetryCount;

        this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
    }

    public AmazonKeyspacesRetryPolicy(DriverContext context, String profileName) {
        DriverExecutionProfile retryExecutionProfile = context.getConfig().getProfile(profileName);

        maxRetryCount = retryExecutionProfile.getInt(AmazonKeyspacesRetryOption.KEYSPACES_RETRY_MAX_ATTEMPTS, AmazonKeyspacesRetryOption.DEFAULT_KEYSPACES_RETRY_MAX_ATTEMPTS);

        this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
    }


    protected RetryDecision determineRetryDecision(int retryCount) {
        if (retryCount < maxRetryCount) {
            return RetryDecision.RETRY_SAME;
        } else {
            return RetryDecision.RETHROW;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation triggers a maximum of configured retry (to the same connection)
     *
     * <p>Otherwise, the exception is rethrown.
     */
    @Override
    public RetryDecision onReadTimeout(
            @NonNull Request request,
            @NonNull ConsistencyLevel cl,
            int blockFor,
            int received,
            boolean dataPresent,
            int retryCount) {

        RetryDecision decision = determineRetryDecision(retryCount);

        LOG.trace(RETRYING_ON_READ_TIMEOUT, logPrefix, cl, blockFor, received, false, retryCount);

        return decision;

    }


    /**
     * {@inheritDoc}
     *
     * <p>This implementation triggers a maximum of configured retry (to the same connection)
     *
     * <p>Otherwise, the exception is rethrown.
     */
    @Override
    public RetryDecision onWriteTimeout(
            @NonNull Request request,
            @NonNull ConsistencyLevel cl,
            @NonNull WriteType writeType,
            int blockFor,
            int received,
            int retryCount) {

        RetryDecision decision = determineRetryDecision(retryCount);

        LOG.trace(RETRYING_ON_WRITE_TIMEOUT, logPrefix, cl, blockFor, received, false, retryCount);

        return decision;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation triggers a maximum of configured retry (to the same connection)
     *
     * <p>Otherwise, the exception is rethrown.
     */
    @Override
    public RetryDecision onUnavailable(
            @NonNull Request request,
            @NonNull ConsistencyLevel cl,
            int required,
            int alive,
            int retryCount) {

        RetryDecision decision = determineRetryDecision(retryCount);

        LOG.trace(RETRYING_ON_UNAVAILABLE, logPrefix, cl, required, alive, retryCount);

        return decision;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation triggers a maximum of configured retry (to the same connection)
     */
    @Override
    public RetryDecision onRequestAborted(
            @NonNull Request request, @NonNull Throwable error, int retryCount) {

        RetryDecision decision = determineRetryDecision(retryCount);

        LOG.trace(RETRYING_ON_ABORTED, logPrefix, retryCount, error);

        return decision;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation triggers a maximum of configured retry (to the same connection)
     */
    @Override
    public RetryDecision onErrorResponse(
            @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {

        RetryDecision decision = determineRetryDecision(retryCount);

        LOG.trace(RETRYING_ON_ERROR, logPrefix, retryCount, error);

        return decision;
    }

    @Override
    public void close() {
        // nothing to do
    }
}