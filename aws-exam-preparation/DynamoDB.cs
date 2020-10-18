using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using FluentAssertions;
using NUnit.Framework;

namespace aws_exam_preparation
{
    class DynamoDB
    {
       // [OneTimeSetUp]
        public async Task CreateTableAndIndexes()
        {
            await DynamoDBHelper.DeleteTable("GameScores");

            var gsi = new GlobalSecondaryIndex()
            {
                IndexName = "GameTitleIndex",
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement("GameTitle", KeyType.HASH),
                    new KeySchemaElement("TopScore", KeyType.RANGE)
                },
                Projection = new Projection()
                {
                    ProjectionType = ProjectionType.ALL,
                },
                ProvisionedThroughput = new ProvisionedThroughput(2, 2)
            };


            var request = new CreateTableRequest()
            {
                TableName = "GameScores",
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition("UserId", ScalarAttributeType.S),
                    new AttributeDefinition("GameTitle", ScalarAttributeType.S),
                    new AttributeDefinition("TopScore", ScalarAttributeType.N)
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement("UserId", KeyType.HASH),
                    new KeySchemaElement("GameTitle", KeyType.RANGE)
                },
                ProvisionedThroughput = new ProvisionedThroughput(2, 2),
                GlobalSecondaryIndexes = {gsi},
                BillingMode = BillingMode.PROVISIONED
            };

            var result = await DynamoDBHelper.Client.CreateTableAsync(request);

            await DynamoDBHelper.WaitUntilTableReady("GameScores");

            var batchResult = await PutItemsBatch(CreateTableItems());

            var results = await PutItems(CreateTableItems());

            var results1 = await PutItems(CreateTableItems());

        }

        [Test]
        public async Task QueryTable()
        {
            var queryRequest = new QueryRequest("GameScores")
            {
                KeyConditionExpression = "UserId = :v_UserId and GameTitle = :v_GameTitle",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    { ":v_UserId", new AttributeValue(){S = "101"}},
                    { ":v_GameTitle", new AttributeValue(){S = "Galaxy Invaders"}}
                },
              
                ReturnConsumedCapacity = ReturnConsumedCapacity.INDEXES
            };

            var result = await DynamoDBHelper.Client.QueryAsync(queryRequest);

            var resultItem = result.Items.Single();

            resultItem["UserId"].S.Should().Be("101");
            resultItem["GameTitle"].S.Should().Be("Galaxy Invaders");
            resultItem["TopScore"].S.Should().Be("28988");
        }

        [Test]
        public async Task ScanTable()
        {
            var scanRequest = new ScanRequest()
            {
                TableName = "GameScores",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    { ":v_GameTitle", new AttributeValue(){S = "Galaxy Invaders"}}
                },
                FilterExpression = "GameTitle = :v_GameTitle",
                ConsistentRead = true
            };

            var result = await DynamoDBHelper.Client.ScanAsync(scanRequest);

            result.Items.Count.Should().Be(3);
        }

        [Test]
        public async Task UpdateItem()
        {
            var updateItemRequest = new UpdateItemRequest()
            {
                TableName = "GameScores",
                Key = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue() {S = "101"}},
                    {"GameTitle", new AttributeValue() {S = "Galaxy Invaders"}}
                },
                AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                {
                    {"TopScore", new AttributeValueUpdate() {Action = "ADD", Value = new AttributeValue() {N = "1"}}}
                }, ReturnValues = ReturnValue.ALL_NEW
            };

            var result = await DynamoDBHelper.Client.UpdateItemAsync(updateItemRequest);
        }


        [Test]
        public void ConsistentReadScanGsi()
        {
            var scanRequest = new ScanRequest()
            {
                TableName = "GameScores",
                IndexName = "GameTitleIndex",
                ConsistentRead = true
            };
            //Amazon.DynamoDBv2.AmazonDynamoDBException: 'Consistent reads are not supported on global secondary indexes'
            Assert.ThrowsAsync<AmazonDynamoDBException>(async () => await DynamoDBHelper.Client.ScanAsync(scanRequest));
        }

        [Test]
        //[Ignore("Doesn't work as index creation takes some time")]
        public async Task QueryGSI()
        {
            await Task.Delay(2000);

            var queryRequest = new QueryRequest("GameScores")
            {
                IndexName = "GameTitleIndex",
                KeyConditionExpression = "GameTitle = :v_GameTitle",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    { ":v_GameTitle", new AttributeValue(){S = "Galaxy Invaders"}}
                },

                ReturnConsumedCapacity = ReturnConsumedCapacity.INDEXES,
                ScanIndexForward = false
            };

            var result = await DynamoDBHelper.Client.QueryAsync(queryRequest);

            result.Items[0]["TopScore"].N.Should().Be("28988");
        }

        private static IEnumerable<PutItemRequest> CreateTableItems()
        {
            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.INDEXES,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "101"}},
                    {"GameTitle", new AttributeValue {S = "Galaxy Invaders"}},
                    {"TopScore", new AttributeValue {N = "28988"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "100"}},
                    {"Losses", new AttributeValue {N = "54"}},
                },
                //ConditionExpression = "attribute_not_exists(UserId)"
            };

            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "101"}},
                    {"GameTitle", new AttributeValue {S = "Meteor Blasters"}},
                    {"TopScore", new AttributeValue {N = "2388"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "222"}},
                    {"Losses", new AttributeValue {N = "5454"}},
                },
                //ConditionExpression = "attribute_not_exists(UserId)"
            };

            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "101"}},
                    {"GameTitle", new AttributeValue {S = "Starship X"}},
                    {"TopScore", new AttributeValue {N = "443"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "66"}},
                    {"Losses", new AttributeValue {N = "78"}},
                },
               //ConditionExpression = "attribute_not_exists(UserId)"
            };

            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "102"}},
                    {"GameTitle", new AttributeValue {S = "Alien Adventure"}},
                    {"TopScore", new AttributeValue {N = "888"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "222"}},
                    {"Losses", new AttributeValue {N = "5454"}},
                }
            };

            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "102"}},
                    {"GameTitle", new AttributeValue {S = "Galaxy Invaders"}},
                    {"TopScore", new AttributeValue {N = "566"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "89987"}},
                    {"Losses", new AttributeValue {N = "4456"}},
                }
            };

            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "103"}},
                    {"GameTitle", new AttributeValue {S = "Attack Ships"}},
                    {"TopScore", new AttributeValue {N = "34343"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "22"}},
                    {"Losses", new AttributeValue {N = "1"}},
                }
            };

            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "103"}},
                    {"GameTitle", new AttributeValue {S = "Galaxy Invaders"}},
                    {"TopScore", new AttributeValue {N = "443"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "66"}},
                    {"Losses", new AttributeValue {N = "78"}},
                }
            };

            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "103"}},
                    {"GameTitle", new AttributeValue {S = "Alien Adventure"}},
                    {"TopScore", new AttributeValue {N = "888"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "222"}},
                    {"Losses", new AttributeValue {N = "5454"}},
                }
            };

            yield return new PutItemRequest
            {
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
                TableName = "GameScores",
                Item = new Dictionary<string, AttributeValue>()
                {
                    {"UserId", new AttributeValue {S = "103"}},
                    {"GameTitle", new AttributeValue {S = "Meteor Blasters"}},
                    {"TopScore", new AttributeValue {N = "345534534"}},
                    {"TopScoreDateTime", new AttributeValue {S = DateTime.Now.ToShortDateString()}},
                    {"Wins", new AttributeValue {N = "33"}},
                    {"Losses", new AttributeValue {N = "4456"}},
                }
            };
        }

        private static async Task<IEnumerable<PutItemResponse>> PutItems(IEnumerable<PutItemRequest> requests)
        {
            var results = new List<PutItemResponse>();
            foreach (var putItemRequest in requests)
            {
                var result = await DynamoDBHelper.Client.PutItemAsync(putItemRequest);
                results.Add(result);
            }

            return results;
        }


        private static async Task<BatchWriteItemResponse> PutItemsBatch(IEnumerable<PutItemRequest> requests)
        {
            var batchRequest = new BatchWriteItemRequest()
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>()
                {
                    { "GameScores", requests.Select(r => new WriteRequest(new PutRequest()
                    {
                        Item = r.Item
                    })).ToList()}
                }, ReturnConsumedCapacity = ReturnConsumedCapacity.INDEXES
            };


            var result = await DynamoDBHelper.Client.BatchWriteItemAsync(batchRequest);

            return result;
        }
    }
}
