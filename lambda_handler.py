import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    print(f"events is {event}")
    try:
        # S3 event notification and start crawler
        if "Records" in event:
            crawler_name = ''
            s3_source = event["Records"][0]["eventSource"]
            s3_event = event["Records"][0]["eventName"]
            print(f"source is {s3_source}")
            print(f"event is {s3_event}")
            if s3_source == "aws:s3" :
                client = boto3.client('glue')
                is_crawler_exist = False
                response = client.list_crawlers()
                if len(response['CrawlerNames']) > 0 :
                    for name in response['CrawlerNames']:
                        print(f"crawler name is {name}")
                        if name == crawler_name:
                            is_crawler_exist = True
                            break

                if is_crawler_exist == False:
                    create_crawler(client)
                else:
                    client.start_crawler(
                        Name=crawler_name
                    )
                return {
                    'statusCode': 200,
                    'body': json.dumps('Crawler has been started Sucessfully')
                }
            else:
                return {
                    'statusCode': 200,
                    'body': json.dumps('Not an S3 event')
                }
        else:
            cloudwatch_link = event['detail']['cloudWatchLogLink']
            crawlerName = event['detail']['crawlerName']
            build_time = event['detail']['completionDate']
            mail_message = (
                    f"{crawlerName} Crawler has been succeeded on {build_time}\n"
                    "\n"
                    f"Kindly visit the below link to view the logs of this build in cloudwatch.\n"
                    f"{cloudwatch_link}"
                    "\n"
                    )
            print(mail_message)
            topic_arn = ''
            mail_subject= "Crawler has been succeeded"
            client = boto3.client('sns')
            client.publish(Message=mail_message,
                        Subject=mail_subject,
                        TopicArn=topic_arn) 
    except Exception as e:
        raise Exception(e)
    
    
def create_crawler(client):
    # Define the crawler configuration
    print("creating crawler")
    crawler_configuration = {
        "Version": 1.0,
        "CreatePartitionIndex":False,
        "CrawlerOutput": {
            "Partitions": {
                "AddOrUpdateBehavior": "InheritFromTable"
            },
            "Tables": {
                "AddOrUpdateBehavior": "MergeNewColumns"
            }
        }
    }

    configuration_json = json.dumps(crawler_configuration)

    response = client.create_crawler(
        Name='test_june12_1',
        Role='arn:aws:iam::123456:role/service-role/AWSGlueServiceRole-test',
        DatabaseName='test',
        Description='crawler for testing',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://bacnet-sc/query/'
                },
            ]
        },
        Schedule='cron(15 12 * * ? *)',
        
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        },
        Configuration=configuration_json,
        Tags={
            'env': 'dev'
        }
    )
