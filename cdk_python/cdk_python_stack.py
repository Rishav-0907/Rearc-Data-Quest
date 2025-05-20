from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3_notifications as s3n,
    Duration,
    RemovalPolicy,
)
from constructs import Construct

class CdkPythonStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket for data storage
        data_bucket = s3.Bucket(
            self, "BLSDataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Create SQS queue for notifications
        queue = sqs.Queue(
            self, "DataAnalysisQueue",
            visibility_timeout=Duration.seconds(300)
        )

        # Create IAM role for Lambda functions
        lambda_role = iam.Role(
            self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ]
        )

        # Add permissions for S3 and SQS
        data_bucket.grant_read_write(lambda_role)
        queue.grant_send_messages(lambda_role)
        queue.grant_consume_messages(lambda_role)

        # Create Lambda function for data sync (Parts 1 & 2)
        data_sync_function = lambda_.Function(
            self, "DataSyncFunction",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="lambda_handlers.data_sync_handler",
            code=lambda_.Code.from_asset("bls_data_sync/src"),
            role=lambda_role,
            environment={
                "S3_BUCKET_NAME": data_bucket.bucket_name,
                "SQS_QUEUE_URL": queue.queue_url
            },
            timeout=Duration.minutes(5)
        )

        # Create Lambda function for data analysis (Part 3)
        analysis_function = lambda_.Function(
            self, "AnalysisFunction",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="lambda_handlers.analysis_handler",
            code=lambda_.Code.from_asset("bls_data_sync/src"),
            role=lambda_role,
            environment={
                "S3_BUCKET_NAME": data_bucket.bucket_name
            },
            timeout=Duration.minutes(5)
        )

        # Add S3 event source for the queue
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="population_data.json")
        )

        # Add SQS event source for the analysis function
        analysis_function.add_event_source(
            lambda_event_sources.SqsEventSource(queue)
        )

        # Create EventBridge rule to trigger data sync daily
        rule = events.Rule(
            self, "DailyDataSyncRule",
            schedule=events.Schedule.rate(Duration.days(1))
        )

        rule.add_target(targets.LambdaFunction(data_sync_function))

        # Output the bucket name and queue URL
        self.bucket_name = data_bucket.bucket_name
        self.queue_url = queue.queue_url 