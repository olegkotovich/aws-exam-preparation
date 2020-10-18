using System;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace dynamodb_streams
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Environment.SetEnvironmentVariable("AWS_PROFILE", "testuser");

            var streamsClient = new AmazonDynamoDBStreamsClient();

            var dynamoDbClient = new AmazonDynamoDBClient();

            var tableDecriptor = await dynamoDbClient.DescribeTableAsync("GameScores");

            var streamArn = tableDecriptor.Table.LatestStreamArn;

            string exclusiveStartShardId = null;

            while (true)
            {
                var result = await streamsClient.DescribeStreamAsync(new DescribeStreamRequest()
                {
                    StreamArn = streamArn,
                    ExclusiveStartShardId = exclusiveStartShardId
                });

                var shards = result.StreamDescription.Shards;

                foreach (var shard in shards)
                {
                    var shardIteratorRequest = new GetShardIteratorRequest()
                    {
                        StreamArn = streamArn,
                        ShardId = shard.ShardId,
                        ShardIteratorType = ShardIteratorType.TRIM_HORIZON
                    };

                    var shardIteratorResponse = await streamsClient.GetShardIteratorAsync(shardIteratorRequest);

                    var currentShardIterator = shardIteratorResponse.ShardIterator;

                    while (currentShardIterator != null )
                    {
                        var recordsResponse = await streamsClient.GetRecordsAsync(new GetRecordsRequest()
                        {
                            ShardIterator = currentShardIterator
                        });

                        foreach (var record in recordsResponse.Records)
                        {
                            var data = record.Dynamodb;

                            Console.WriteLine("Table changed");
                        }

                        currentShardIterator = recordsResponse.NextShardIterator;
                    }
                }
            }
        }
}
}
