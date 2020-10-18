using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using NUnit.Framework;

namespace aws_exam_preparation
{
    [TestFixture]
    public class WhenUploadingFileWithSseCEncryption
    {
        private PutObjectResponse _uploadResponse;
        private string _s3Key;

        [OneTimeSetUp]
        public async Task SetUp()
        {
            S3Helper.CreateS3Bucket().Wait();

            _s3Key = $"myFile_sse-c_{Guid.NewGuid().ToString()}.txt";

            File.WriteAllText(_s3Key, "Hello World!");

            _uploadResponse = await S3Helper.Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest()
            {
                BucketName = S3Helper.DefaultBucketName,
                Key = _s3Key,
                ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                ServerSideEncryptionCustomerProvidedKey = "+X/eQksT7boYpJKb9fcSJYZec7HRxm/y2UKaQEhcqWA=",
                FilePath = _s3Key
            });
        }

        [Test]
        public async Task ShouldReturnDecryptedObject()
        {
            var response = await S3Helper.Client.GetObjectAsync(new GetObjectRequest()
            {
                BucketName = S3Helper.DefaultBucketName,
                Key = _s3Key,
                ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                ServerSideEncryptionCustomerProvidedKey = "+X/eQksT7boYpJKb9fcSJYZec7HRxm/y2UKaQEhcqWA=",
            });

            response.ServerSideEncryptionCustomerMethod.Value.Should().Be("AES256");
        }

        [TestFixture]
        public class WhenUploadingFileWithSseKmsEncryption
        {
            private PutObjectResponse _uploadResponse;
            private string _s3Key;

            [OneTimeSetUp]
            public async Task SetUp()
            {
                await S3Helper.CreateS3Bucket();

                _s3Key = $"myFile_sse-c_{Guid.NewGuid().ToString()}.txt";

                File.WriteAllText(_s3Key, "Hello World!");

                _uploadResponse = await S3Helper.Client.PutObjectAsync(new PutObjectRequest()
                {
                    BucketName = S3Helper.DefaultBucketName,
                    Key = _s3Key,
                    ServerSideEncryptionKeyManagementServiceKeyId = S3Helper.KmsKeyId,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS,
                    FilePath = _s3Key
                });
            }

            [Test]
            public async Task ShouldReturnDecryptedObject()
            {
                var response = await S3Helper.Client.GetObjectAsync(new GetObjectRequest()
                {
                    BucketName = S3Helper.DefaultBucketName,
                    Key = _s3Key
                });

                response.ServerSideEncryptionMethod.Value.Should().Be("aws:kms");
            }
        }

        [TestFixture]
        public class WhenUploadingFileWithS3Encryption
        {
            private PutObjectResponse _uploadResponse;
            private string _s3Key;

            [OneTimeSetUp]
            public async Task SetUp()
            {
                await S3Helper.CreateS3Bucket();

                _s3Key = $"myFile_sse-c_{Guid.NewGuid().ToString()}.txt";

                File.WriteAllText(_s3Key, "Hello World!");

                _uploadResponse = await S3Helper.Client.PutObjectAsync(new PutObjectRequest()
                {
                    BucketName = S3Helper.DefaultBucketName,
                    Key = _s3Key,
                    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256,
                    FilePath = _s3Key
                });
            }

            [Test]
            public async Task ShouldReturnDecryptedObject()
            {
                var response = await S3Helper.Client.GetObjectAsync(new GetObjectRequest()
                {
                    BucketName = S3Helper.DefaultBucketName,
                    Key = _s3Key
                });

                response.ServerSideEncryptionMethod.Value.Should().Be("AES256");
            }
        }
    }
}
