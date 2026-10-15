# IoT Vitals Pipeline: AWS IoT Core + Databricks

A hands-on learning project. A simulated blood-pressure and SpO2 device publishes a reading every 5 seconds. AWS raises an email alert within seconds when SpO2 drops below 85% or systolic pressure reaches 180 or more. The same data lands in S3 and is turned into Bronze, Silver and Gold Delta tables in Databricks Free Edition.

> **Disclaimer:** synthetic data only. This is a learning exercise, not a medical device or clinical alerting system.

![Architecture](docs/architecture.png)

## How it works

| Path | Flow | Speed |
|---|---|---|
| Alert | Device, MQTT/TLS, IoT Core rule 1 (`WHERE spo2 < 85 OR systolic >= 180`), SNS, email | seconds |
| Analytics | Device, IoT Core rule 2 (all readings), Kinesis Data Firehose, S3 JSON files, Databricks | 1 to 2 minutes |

Why the alert is in AWS and not Databricks: Databricks Free Edition restricts outbound network access and is not built for always-on streaming, so the time-critical alert sits where the data first arrives. Databricks does history, reconciliation and trend analysis.

## Repository contents

```
simulator.py                      device simulator (MQTT with certificates, or HTTPS fallback)
setup_aws.py                      optional: creates and tears down all AWS resources with boto3
databricks_vitals_notebook.py     Databricks notebook (import as a notebook): S3 to Bronze/Silver/Gold
docs/                             high-level design (Word), step-by-step guides, architecture diagram
docs/evidence/                    redacted alert emails and Databricks screenshots
```

## AWS resources (all in one region, default eu-west-2)

S3 bucket with a 7-day lifecycle rule, SNS topic with an email subscription, three least-privilege IAM roles, a Firehose stream (60 s buffer), an IoT thing with certificate and device policy, and two IoT rules. Build them by hand in the console (see `docs/`) or run `python setup_aws.py setup --email you@example.com`.

## Run it

```bash
pip install awsiotsdk boto3
python simulator.py --endpoint <your-endpoint>-ats.iot.eu-west-2.amazonaws.com
```

Defaults: one reading every 5 seconds for up to 30 minutes, with short abnormal episodes injected periodically so you can see the alerts fire. Change this with `--interval` and `--duration-min`. Use `--mode boto3` if you have no certificates, and `--mode print` to test with no network.

Then in Databricks: import `databricks_vitals_notebook.py` into your workspace, attach serverless compute, fill in the widgets (bucket, region, read-only key pair) and run the cells.

## Databricks tables

| Table | Contents |
|---|---|
| `bronze_vitals` | raw readings as landed plus source file |
| `silver_vitals` | typed, de-duplicated, event/ingest time, validity flag |
| `gold_alerts` | readings that crossed the thresholds (reconciles with emails received) |
| `gold_trend_warnings` | 30-second moving-average warnings |
| `gold_hourly_summary` | per device per hour statistics |

## Results (from a real run)

| Check | Result |
|---|---|
| Readings landed in Bronze | 248 (244 from the simulator, 3 manual console test messages, 1 duplicate) |
| Readings in Silver | 247 (1 duplicate removed on device + timestamp) |
| Gold alert readings | 15: 10 `LOW_SPO2`, 5 `HIGH_BP` |
| Alert emails received | 15 (matches Gold; 2 came from manual console tests, 13 from the simulator) |

The real-time email path and the Databricks analytics path agree. Evidence in [`docs/evidence/`](docs/evidence/): [alert emails (personal details redacted)](docs/evidence/00_alert_emails_redacted.pdf), [row counts](docs/evidence/01_row_counts.png), [gold alerts](docs/evidence/02_gold_alerts.png), [hourly summary](docs/evidence/03_gold_hourly_summary.png).

Note: the device clock and AWS clock were not synchronised in this run, so end-to-end latency was not measured.

Design write-up: [`docs/IoT_Vitals_HLD.docx`](docs/IoT_Vitals_HLD.docx).

## Security

- Device certificates and keys are in `.gitignore`. **Never commit `certs/`, `*.pem`, `*.key`, access keys or `.csv` credential files.**
- The device policy allows connecting only as its own client ID and publishing only to its own topic.
- Databricks uses a read-only IAM user limited to the bucket. Delete it when finished.

## Cost control

A short run of a few minutes is only a few dozen messages. Run short tests, then tear everything down (`python setup_aws.py teardown` or the manual cleanup steps in `docs/`).

## Limitations and next steps

- Synthetic data, one device, Databricks Free Edition (serverless only).
- Each abnormal reading sends its own email. Production would add a cooldown (Lambda + DynamoDB) to avoid alert fatigue.
- Next: Auto Loader for incremental loads, Parquet from Firehose, infrastructure as code (CloudFormation or Terraform), CloudWatch alarms and a rule error action, and a dashboard on the Gold tables.
