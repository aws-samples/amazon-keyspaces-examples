using System;

using Xunit;
using Amazon.Lambda.TestUtilities;
using Amazon.Lambda.SQSEvents;

namespace keyspaces_lambda_csharp.Tests
{
    /*
     * Test class to validate the Lambda function handler locally.
     * Run this to test the code before running any included scripts: 1-create-bucket.sh, 2-deploy.sh etc.
     * 
     * Set the environment variables (AWS_REGION, SERVICE_USER_NAME, SERVICE_PASSWORD and CERTIFICATE_PATH) before running this test.
     * E.g.:
     * AWS_REGION=us-east-1
     * SERVICE_USER_NAME=alice-at-111122223333
     * SERVICE_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
     * CERTIFICATE_PATH=/Users/alice/Projects/keyspaces-lambda-csharp
     */
    public class LambdaSampleTest
    {
        [Fact]
        public void TestFunction()
        {
            Console.WriteLine("Running Invoke Test");
            var sample = new LambdaSample();
            var context = new TestLambdaContext();
            SQSEvent input = new SQSEvent();
            string output = sample.FunctionHandler(input, context);
            Console.WriteLine(output);
            Assert.Equal("200 OK", output);
        }
    }
}
