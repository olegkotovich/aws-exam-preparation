using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;

namespace aws_exam_preparation
{
    public static class S3Helper
    {
        public const string DefaultBucketName = "my-test-bucket-olegkotovich";
        public const string KmsKeyId = "bb4682c7-5cfe-4fa1-989e-f0c01869835a";

        public static AmazonS3Client Client = new AmazonS3Client();

        public static async Task<bool> CreateS3Bucket(string bucketName = DefaultBucketName)
        {
            var bucketList = await Client.ListBucketsAsync();

            if (!bucketList.Buckets.ToArray().Any(i => i.BucketName.Equals(bucketName, StringComparison.InvariantCultureIgnoreCase)))
            {
                await Client.PutBucketAsync(bucketName);

                return true;
            }

            return false;
        }

    }
}
