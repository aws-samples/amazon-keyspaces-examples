/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: MIT-0
 */

package example;

import java.util.HashMap;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for verfiying the handler locally.
 *
 * Run this to ensure that the code is working before using the included scripts.
 */
class InvokeTest {
  private static final Logger logger = LoggerFactory.getLogger(InvokeTest.class);

  @Test
  void invokeTest() {
    logger.info("Invoke TEST");
    HashMap<String,String> event = new HashMap<String,String>();
    Context context = new TestContext();
    Handler handler = new Handler();
    String result = handler.handleRequest(event, context);
    assertTrue(result.contains("200 OK"));
  }
}
