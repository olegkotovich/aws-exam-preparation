using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace aws_exam_preparation
{
    public static class DynamoDBHelper
    {
        public static AmazonDynamoDBClient Client = new AmazonDynamoDBClient();

        public static async Task WaitUntilTableReady(string tableName)
        {
            string status = null;
            do
            {
                await Task.Delay(1000);

                try
                {
                    var res = await Client.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = tableName
                    });

                    Console.WriteLine("Table name: {0}, status: {1}",
                        res.Table.TableName,
                        res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }
                catch (ResourceNotFoundException)
                {
                    // DescribeTable is eventually consistent. So you might
                    // get resource not found. So we handle the potential exception.
                }
            } while (status != "ACTIVE");
        }

        public static async Task WaitUntilTableDeleted(string tableName)
        {
            do
            {
                await Task.Delay(1000);

                try
                {
                    await Client.DescribeTableAsync(tableName);
                }
                catch (ResourceNotFoundException)
                {
                    break;
                }
            } while (true);
        }

        public static async Task DeleteTable(string tableName)
        {
            try
            {
                var response = await Client.DeleteTableAsync(tableName);
                await WaitUntilTableDeleted(tableName);
            }
            catch (ResourceNotFoundException e)
            {
                Console.WriteLine(e);
            }
        }

    }
}
