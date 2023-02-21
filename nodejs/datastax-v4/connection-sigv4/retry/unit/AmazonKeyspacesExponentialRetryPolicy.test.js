/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: MIT-0
 */
'use strict';
const assert = require('assert');
const cassandra = require('cassandra-driver');
const types = cassandra.types;
const policies = cassandra.policies;
const custom_retry = require('../AmazonKeyspacesExponentialRetryPolicy.js');
const RetryPolicy = cassandra.policies.retry.RetryPolicy;

describe('AmazonKeyspacesExponentialRetryPolicy', function () {
  describe('#onUnavailable()', function () {
    it('should retry on the same host for onUnavailable exception', function () {
      const policy = new custom_retry.AmazonKeyspacesExponentialRetryPolicy();
      const result = policy.onUnavailable(getRequestInfo(2), types.consistencies.one, 1, 1);
      assert.strictEqual(result.consistency,  types.consistencies.one);
      assert.strictEqual(result.decision, RetryPolicy.retryDecision.retry);
      assert.strictEqual(result.useCurrentHost, true);
    });
    it('should rethrow the error after Max retries', function () {
      const policy = new custom_retry.AmazonKeyspacesExponentialRetryPolicy();
      const result = policy.onUnavailable(getRequestInfo(4), types.consistencies.one, 1, 1);
      assert.strictEqual(result.decision, RetryPolicy.retryDecision.rethrow);
    });
  });
  describe('#onWriteTimeout()', function () {
    it('should retry on the same host for the WriteTimeout', function () {
      const policy = new custom_retry.AmazonKeyspacesExponentialRetryPolicy();
      const result = policy.onWriteTimeout(getRequestInfo(1), types.consistencies.localQuorum, 2, 2, 'SIMPLE');
      assert.strictEqual(result.consistency,  types.consistencies.localQuorum);
      assert.strictEqual(result.decision, RetryPolicy.retryDecision.retry);
      assert.strictEqual(result.useCurrentHost, true);
    });
    it('should rethrow the error after Max retries', function () {
      const policy = new custom_retry.AmazonKeyspacesExponentialRetryPolicy();
      const result = policy.onWriteTimeout(getRequestInfo(4), types.consistencies.localQuorum, 2, 2, 'SIMPLE');
      assert.strictEqual(result.decision, RetryPolicy.retryDecision.rethrow);
    });
  });
  describe('#onReadTimeout()', function () {
    it('should retry on the same host the for the Read Timeout', function () {
      const policy = new custom_retry.AmazonKeyspacesExponentialRetryPolicy();
      const result = policy.onReadTimeout(getRequestInfo(2), types.consistencies.localQuorum, 2, 2, false);
      assert.strictEqual(result.consistency,  types.consistencies.localQuorum);
      assert.strictEqual(result.decision, RetryPolicy.retryDecision.retry);
      assert.strictEqual(result.useCurrentHost, true);
    });
    it('should rethrow the error after Max retries', function () {
      const policy = new custom_retry.AmazonKeyspacesExponentialRetryPolicy();
      const result = policy.onReadTimeout(getRequestInfo(4), types.consistencies.localQuorum, 2, 2, false);
      assert.strictEqual(result.decision, RetryPolicy.retryDecision.rethrow);
    });
  });
  describe('#onRequestError()', function () {
    it('should retry on the same host', function () {
      const policy = new custom_retry.AmazonKeyspacesExponentialRetryPolicy();
      const result = policy.onRequestError(getRequestInfo(1), types.consistencies.one, 1, 1, false);
      assert.strictEqual(result.consistency,  types.consistencies.one);
      assert.strictEqual(result.decision, RetryPolicy.retryDecision.retry);
      assert.strictEqual(result.useCurrentHost, true);
    });
    it('should rethrow the error after Max retries', function () {
      const policy = new custom_retry.AmazonKeyspacesExponentialRetryPolicy();
      const result = policy.onRequestError(getRequestInfo(4), types.consistencies.one, 1, 1, false);
      assert.strictEqual(result.decision, RetryPolicy.retryDecision.rethrow);
    });
  });

});
function getRequestInfo(nbRetry) {
    return {
      nbRetry: nbRetry || 0,
      query: 'SAMPLE'
    };
}