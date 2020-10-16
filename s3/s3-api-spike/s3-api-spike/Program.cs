using Amazon.KeyManagementService;
using Amazon.S3;
using System;
using System.IO;
using System.Linq;

namespace s3_api_spike
{
    class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            const string bucketName = "my-test-bucket-olegkotovich";

            var s3Client = new AmazonS3Client();

            var bucketList = await s3Client.ListBucketsAsync();

            if (!bucketList.Buckets.ToArray().Any(i => i.BucketName.Equals(bucketName, StringComparison.InvariantCultureIgnoreCase)))
            {
                await s3Client.PutBucketAsync(bucketName);
            }

            var folderPath = "testdata";
            if (Directory.Exists(folderPath))
            {
                Directory.Delete(folderPath, true);
            }

            Directory.CreateDirectory(folderPath);

            Enumerable.Range(0, 5).ToList().ForEach(i => GenerateFile(1, Path.Combine(folderPath, Guid.NewGuid().ToString())));


            //SSE-C Encryption
            var s3Key = $"myFile_sse-c_{Guid.NewGuid().ToString()}.txt";

            File.WriteAllText(s3Key, "Hello World!");

            var ssecResponse = await s3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest()
            {
                BucketName = bucketName,
                Key = s3Key,
                ServerSideEncryptionCustomerMethod = ServerSideEncryptionCustomerMethod.AES256,
                ServerSideEncryptionCustomerProvidedKey = "+X/eQksT7boYpJKb9fcSJYZec7HRxm/y2UKaQEhcqWA=", 
                FilePath = s3Key
            });


            //SSE-KMS 

            var s3KeyKms = $"myFile_sse-kms_{Guid.NewGuid()}.txt";

            GenerateFile(1, s3KeyKms);

            var ssekmsResponse = await s3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest()
            {
                BucketName = bucketName,
                Key = s3KeyKms,
                ServerSideEncryptionKeyManagementServiceKeyId = "bb4682c7-5cfe-4fa1-989e-f0c01869835a",
                ServerSideEncryptionMethod = ServerSideEncryptionMethod.AWSKMS,
                FilePath = s3KeyKms
            });


            var kmsClient = new AmazonKeyManagementServiceClient();

            var dataKey = await kmsClient.GenerateDataKeyAsync(new Amazon.KeyManagementService.Model.GenerateDataKeyRequest()
            { 
                KeyId = "arn:aws:kms:eu-central-1:655124928368:key/bb4682c7-5cfe-4fa1-989e-f0c01869835a", 
                KeySpec = DataKeySpec.AES_256
            });

            var decryptedDataKey = await kmsClient.DecryptAsync(new Amazon.KeyManagementService.Model.DecryptRequest()
            {
                CiphertextBlob = dataKey.CiphertextBlob, KeyId = dataKey.KeyId
            });

            var decryptedPlainTextKey = Convert.ToBase64String(decryptedDataKey.Plaintext.ToArray());

            var plainTextKey = Convert.ToBase64String(dataKey.Plaintext.ToArray());

        }

        private static void GenerateFile(int sizeInMb, string fileName)
        {
            byte[] data = new byte[sizeInMb * 1024 * 1024];
            Random rng = new Random();
            rng.NextBytes(data);
            File.WriteAllBytes(fileName, data);
        }

    }
}
