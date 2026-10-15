#!/usr/bin/env python3
"""
Creates (and later deletes) every AWS resource for the IoT vitals project.

  python setup_aws.py setup    --email you@example.com [--region eu-west-2]
  python setup_aws.py teardown [--region eu-west-2]

Creates:
  S3 bucket (7-day lifecycle) | SNS topic + email subscription | 3 IAM roles
  Firehose stream (JSON -> S3, 60 s buffer) | IoT thing + certificate + policy
  IoT rule 1  vitals_alert_rule   : spo2 < 85 OR systolic >= 180  -> SNS (email)
  IoT rule 2  vitals_to_s3_rule   : every reading                 -> Firehose -> S3
  Optional AWS Budget alert (USD 5) with --budget

Needs:  pip install boto3   and AWS credentials with admin-like rights in a
        sandbox/learning account (aws configure, or environment variables).
Never commit the certs/ folder or your access keys to Git or share them.
"""
import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

DEVICE = "bp-oximeter-001"
TOPIC_NAME = "vitals-alerts"
STREAM = "vitals-stream"
ALERT_RULE = "vitals_alert_rule"
S3_RULE = "vitals_to_s3_rule"
POLICY = "vitals-device-policy"
ROLE_IOT_SNS = "vitals-iot-to-sns-role"
ROLE_IOT_FH = "vitals-iot-to-firehose-role"
ROLE_FH_S3 = "vitals-firehose-to-s3-role"
BUDGET = "vitals-demo-budget"
ROOT_CA_URL = "https://www.amazontrust.com/repository/AmazonRootCA1.pem"


def trust(service):
    return json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]})


def retry(fn, tries=8, delay=5, retry_on=("InvalidArgumentException", "InvalidRequestException",
                                          "MalformedPolicyDocument", "ResourceNotFoundException")):
    """IAM roles take a few seconds to become usable by other services."""
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in retry_on and i < tries - 1:
                print(f"   ...waiting for IAM to propagate ({code}), retry {i + 1}")
                time.sleep(delay)
                continue
            raise


def ensure_role(iam, name, service, policy_name, policy_doc):
    try:
        arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust(service))["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        arn = iam.get_role(RoleName=name)["Role"]["Arn"]
    iam.put_role_policy(RoleName=name, PolicyName=policy_name, PolicyDocument=json.dumps(policy_doc))
    return arn


def setup(a):
    sess = boto3.Session(region_name=a.region)
    acct = sess.client("sts").get_caller_identity()["Account"]
    region = a.region
    s3, sns, iam = sess.client("s3"), sess.client("sns"), sess.client("iam")
    fh, iot = sess.client("firehose"), sess.client("iot")
    bucket = f"vitals-demo-{acct}-{region}"

    print("1/7 S3 bucket", bucket)
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou",):
            raise
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": [
        {"ID": "expire-demo-data", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": {"Days": 7}}]})

    print("2/7 SNS topic + email subscription (CHECK YOUR INBOX and click Confirm)")
    topic_arn = sns.create_topic(Name=TOPIC_NAME)["TopicArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=a.email)

    print("3/7 IAM roles")
    r_sns = ensure_role(iam, ROLE_IOT_SNS, "iot.amazonaws.com", "publish-alerts", {
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sns:Publish", "Resource": topic_arn}]})
    stream_arn = f"arn:aws:firehose:{region}:{acct}:deliverystream/{STREAM}"
    r_fh_in = ensure_role(iam, ROLE_IOT_FH, "iot.amazonaws.com", "put-firehose", {
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["firehose:PutRecord", "firehose:PutRecordBatch"],
                                                "Resource": stream_arn}]})
    r_fh_s3 = ensure_role(iam, ROLE_FH_S3, "firehose.amazonaws.com", "write-s3", {
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": [
            "s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket",
            "s3:ListBucketMultipartUploads", "s3:PutObject"],
            "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]}]})

    print("4/7 Firehose stream (JSON lines -> S3, flushes every 60 s)")
    try:
        retry(lambda: fh.create_delivery_stream(
            DeliveryStreamName=STREAM, DeliveryStreamType="DirectPut",
            ExtendedS3DestinationConfiguration={
                "RoleARN": r_fh_s3, "BucketARN": f"arn:aws:s3:::{bucket}",
                "Prefix": "bronze/vitals/dt=!{timestamp:yyyy-MM-dd}/",
                "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 60},
                "CompressionFormat": "UNCOMPRESSED"}))
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    for _ in range(30):
        st = fh.describe_delivery_stream(DeliveryStreamName=STREAM)["DeliveryStreamDescription"]["DeliveryStreamStatus"]
        if st == "ACTIVE":
            break
        time.sleep(4)

    print("5/7 IoT thing, certificate and least-privilege policy")
    certs = Path(a.certs)
    certs.mkdir(exist_ok=True)
    try:
        iot.create_thing(thingName=DEVICE)
    except ClientError:
        pass
    pol = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "iot:Connect", "Resource": f"arn:aws:iot:{region}:{acct}:client/{DEVICE}"},
        {"Effect": "Allow", "Action": "iot:Publish", "Resource": f"arn:aws:iot:{region}:{acct}:topic/medical/{DEVICE}/vitals"}]}
    try:
        iot.create_policy(policyName=POLICY, policyDocument=json.dumps(pol))
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    if not (certs / "device.pem.crt").exists():
        c = iot.create_keys_and_certificate(setAsActive=True)
        (certs / "device.pem.crt").write_text(c["certificatePem"])
        (certs / "private.pem.key").write_text(c["keyPair"]["PrivateKey"])
        iot.attach_policy(policyName=POLICY, target=c["certificateArn"])
        iot.attach_thing_principal(thingName=DEVICE, principal=c["certificateArn"])
        (certs / "cert_arn.txt").write_text(c["certificateArn"])
    try:
        urllib.request.urlretrieve(ROOT_CA_URL, str(certs / "AmazonRootCA1.pem"))
    except Exception as e:  # noqa: BLE001
        print("   Could not download the root CA automatically:", e, "\n   Download it from", ROOT_CA_URL,
              "and save it as certs/AmazonRootCA1.pem")

    print("6/7 IoT rules")
    alert_sql = (
        "SELECT deviceId, ts, spo2, systolic, diastolic, heartRate "
        "FROM 'medical/+/vitals' WHERE spo2 < 85 OR systolic >= 180")
    store_sql = "SELECT *, timestamp() AS ingestTs FROM 'medical/+/vitals'"
    rules = [
        (ALERT_RULE, alert_sql, {"sns": {"targetArn": topic_arn, "roleArn": r_sns, "messageFormat": "RAW"}}),
        (S3_RULE, store_sql, {"firehose": {"roleArn": r_fh_in, "deliveryStreamName": STREAM, "separator": "\n"}}),
    ]
    for name, sql, action in rules:
        payload = {"sql": sql, "awsIotSqlVersion": "2016-03-23", "ruleDisabled": False, "actions": [action]}
        try:
            retry(lambda: iot.create_topic_rule(ruleName=name, topicRulePayload=payload))
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
                iot.replace_topic_rule(ruleName=name, topicRulePayload=payload)
            else:
                raise

    print("7/7 Budget alert" if a.budget else "7/7 (budget skipped - add --budget to create a USD 5 alert)")
    if a.budget:
        b = sess.client("budgets", region_name="us-east-1")
        try:
            b.create_budget(AccountId=acct, Budget={
                "BudgetName": BUDGET, "BudgetType": "COST", "TimeUnit": "MONTHLY",
                "BudgetLimit": {"Amount": "5", "Unit": "USD"}},
                NotificationsWithSubscribers=[{"Notification": {
                    "NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN",
                    "Threshold": 80, "ThresholdType": "PERCENTAGE"},
                    "Subscribers": [{"SubscriptionType": "EMAIL", "Address": a.email}]}])
        except ClientError as e:
            print("   Budget not created:", e.response["Error"]["Message"])

    endpoint = iot.describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    print("\nDONE. Next steps")
    print("  1. Confirm the SNS subscription email.")
    print(f"  2. Run:  python simulator.py --endpoint {endpoint} --certs {a.certs} --region {region}")
    print(f"  3. Data lands in s3://{bucket}/bronze/vitals/ about a minute after publishing.")
    print("  4. When finished:  python setup_aws.py teardown --region", region)


def teardown(a):
    sess = boto3.Session(region_name=a.region)
    acct = sess.client("sts").get_caller_identity()["Account"]
    s3, sns, iam = sess.client("s3"), sess.client("sns"), sess.client("iam")
    fh, iot = sess.client("firehose"), sess.client("iot")
    bucket = f"vitals-demo-{acct}-{a.region}"

    def safe(label, fn):
        try:
            fn()
            print("deleted", label)
        except ClientError as e:
            print("skip   ", label, "-", e.response["Error"]["Code"])

    for r in (ALERT_RULE, S3_RULE):
        safe(f"IoT rule {r}", lambda r=r: iot.delete_topic_rule(ruleName=r))
    cert_file = Path(a.certs) / "cert_arn.txt"
    if cert_file.exists():
        arn = cert_file.read_text().strip()
        cid = arn.split("/")[-1]
        safe("detach policy", lambda: iot.detach_policy(policyName=POLICY, target=arn))
        safe("detach thing", lambda: iot.detach_thing_principal(thingName=DEVICE, principal=arn))
        safe("deactivate cert", lambda: iot.update_certificate(certificateId=cid, newStatus="INACTIVE"))
        safe("delete cert", lambda: iot.delete_certificate(certificateId=cid, forceDelete=True))
    safe("IoT policy", lambda: iot.delete_policy(policyName=POLICY))
    safe("IoT thing", lambda: iot.delete_thing(thingName=DEVICE))
    safe("Firehose stream", lambda: fh.delete_delivery_stream(DeliveryStreamName=STREAM))
    topic_arn = f"arn:aws:sns:{a.region}:{acct}:{TOPIC_NAME}"
    safe("SNS topic", lambda: sns.delete_topic(TopicArn=topic_arn))
    for role, pol in ((ROLE_IOT_SNS, "publish-alerts"), (ROLE_IOT_FH, "put-firehose"), (ROLE_FH_S3, "write-s3")):
        safe(f"role policy {role}", lambda role=role, pol=pol: iam.delete_role_policy(RoleName=role, PolicyName=pol))
        safe(f"role {role}", lambda role=role: iam.delete_role(RoleName=role))

    def empty_and_delete():
        bkt = sess.resource("s3").Bucket(bucket)
        bkt.objects.all().delete()
        s3.delete_bucket(Bucket=bucket)
    safe(f"S3 bucket {bucket}", empty_and_delete)
    safe("budget", lambda: sess.client("budgets", region_name="us-east-1").delete_budget(AccountId=acct, BudgetName=BUDGET))
    print("Teardown finished. Also delete the local certs/ folder and any IAM user/keys you created for Databricks.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["setup", "teardown"])
    ap.add_argument("--email", help="address for alert emails (setup)")
    ap.add_argument("--region", default="eu-west-2")
    ap.add_argument("--certs", default="certs")
    ap.add_argument("--budget", action="store_true", help="also create a USD 5 monthly budget alert")
    args = ap.parse_args()
    if args.action == "setup":
        if not args.email:
            sys.exit("--email is required for setup")
        setup(args)
    else:
        teardown(args)
