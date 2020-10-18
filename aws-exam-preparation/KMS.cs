using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Amazon.KeyManagementService;
using FluentAssertions;
using NUnit.Framework;

namespace aws_exam_preparation
{
    public class WhenRequestingDataKey
    {
        [Test]
        public async Task ShouldReturnPlainTextAndEncryptedKeys()
        {
            using (var kmsClient = new AmazonKeyManagementServiceClient())
            {
                var dataKey = await kmsClient.GenerateDataKeyAsync(new Amazon.KeyManagementService.Model.GenerateDataKeyRequest()
                {
                    KeyId = "arn:aws:kms:eu-central-1:655124928368:key/bb4682c7-5cfe-4fa1-989e-f0c01869835a",
                    KeySpec = DataKeySpec.AES_256
                });

                var decryptedDataKey = await kmsClient.DecryptAsync(new Amazon.KeyManagementService.Model.DecryptRequest()
                {
                    CiphertextBlob = dataKey.CiphertextBlob,
                    KeyId = "arn:aws:kms:eu-central-1:655124928368:key/bb4682c7-5cfe-4fa1-989e-f0c01869835a"
                });

                var decryptedPlainTextKey = Convert.ToBase64String(decryptedDataKey.Plaintext.ToArray());

                var plainTextKey = Convert.ToBase64String(dataKey.Plaintext.ToArray());

                decryptedPlainTextKey.Should().Be(plainTextKey);
            }

        }
    }
}
