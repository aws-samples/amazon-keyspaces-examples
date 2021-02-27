using System;

using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using keyspaceslambdacsharp;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace keyspaces_lambda_csharp
{
    /*
     * Represents the Lambda Function Handler.
     * Returns "200 OK" if connection with Keyspaces is established successfully.
     * Returns "400 Bad Request" in case of an error/exception.
     */
    public class LambdaSample
    {
        public string FunctionHandler(SQSEvent invocationEvent, ILambdaContext context)
        {
            try
            {
                LambdaLogger.Log("ENVIRONMENT VARIABLES: " + JsonConvert.SerializeObject(Environment.GetEnvironmentVariables()));
                LambdaLogger.Log("CONTEXT: " + JsonConvert.SerializeObject(context));
                LambdaLogger.Log("EVENT: " + JsonConvert.SerializeObject(invocationEvent));

                int result = KeyspacesSession.Instance().validateConnection();
                if (result == 200)
                    return "200 OK";
            }
            catch (Exception e)
            {
                LambdaLogger.Log("Caught an exception: " + e.ToString());
            }
            LambdaLogger.Log("Something went wrong. Please see the exception message and stack trace for more details.");
            return "400 Bad Request";
        }
    }
}
