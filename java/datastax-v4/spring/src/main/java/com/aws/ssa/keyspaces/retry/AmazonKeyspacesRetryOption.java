package com.aws.ssa.keyspaces.retry;

import com.datastax.oss.driver.api.core.config.DriverOption;

public enum AmazonKeyspacesRetryOption implements DriverOption {

    KEYSPACES_RETRY_MAX_ATTEMPTS("advanced.retry-policy.max-attempts");

    public static final Integer DEFAULT_KEYSPACES_RETRY_MAX_ATTEMPTS = 3;

    private final String path;

    AmazonKeyspacesRetryOption(String path) {
        this.path = path;
    }

    @Override
    public String getPath() {
        return path;
    }

}
