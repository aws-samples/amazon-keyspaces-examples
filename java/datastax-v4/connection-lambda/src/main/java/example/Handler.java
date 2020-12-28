/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: MIT-0
 */

package example;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.util.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Represents the Lambda Handler.
 * Note that {@link KeyspacesSSCSession} and {@link KeyspacesSigV4Session}
 * are used as singletons.  Initialization of Java Cassandra Driver takes time, making these
 * singletons ensures that cost is not paid every time the lambda is invoked.
 */
public class Handler implements RequestHandler<Map<String,String>, String>{
  Gson gson = new GsonBuilder().setPrettyPrinting().create();

  @Override
  public String handleRequest(Map<String,String> event, Context context)
  {
    LambdaLogger logger = context.getLogger();

    try {
      logger.log("Printing environment settings:");
      logger.log("CONTEXT: " + gson.toJson(context));

      // process event
      logger.log("EVENT: " + gson.toJson(event));
      logger.log("EVENT TYPE: " + event.getClass().toString());

      int success;
      if (shouldLoginUsingSSC()) {
        success = KeyspacesSSCSession.Instance(logger).validateConnection();
      } else {
        success = KeyspacesSigV4Session.Instance(logger).validateConnection();
      }

      if(success == 200) {
        return "200 OK";
      }

      return  "400 error";
    } catch (Exception e) {
      e.printStackTrace();
      return "400 error";
    }
  }

  private boolean shouldLoginUsingSSC() {
    String sscUserName = System.getenv("SSC_USER_NAME");
    return !StringUtils.isNullOrEmpty(sscUserName);
  }
}