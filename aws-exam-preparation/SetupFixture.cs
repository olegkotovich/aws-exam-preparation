using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Amazon.S3;
using NUnit.Framework;

namespace aws_exam_preparation
{
    [SetUpFixture]
    public class TestInitializer
    {
        [OneTimeSetUp]
        public void Setup()
        {
            Environment.SetEnvironmentVariable("AWS_PROFILE", "testuser");
        }
    }
}
