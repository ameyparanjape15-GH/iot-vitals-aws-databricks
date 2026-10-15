#!/usr/bin/env python3
"""
Synthetic IoT medical-device simulator (LEARNING PROJECT - NOT A MEDICAL DEVICE).

Publishes one reading every 5 seconds:
    {"deviceId", "ts" (epoch ms), "spo2", "systolic", "diastolic", "heartRate"}
to the MQTT topic  medical/<deviceId>/vitals  on AWS IoT Core.

Transports
  --mode mqtt   (default) X.509 certificates + MQTT over TLS (what real devices do)
                pip install awsiotsdk
  --mode boto3  HTTPS publish using your AWS credentials (no certificates needed)
                pip install boto3
  --mode print  No network. Prints the payloads. Use it to test the logic.

Scenarios (to prove the alerts work without waiting for chance):
  normal        healthy readings only
  hypoxia       SpO2 drops below 85% in short episodes
  hypertension  systolic rises to 180+ in short episodes
  mixed         both kinds of episode (default)
Episodes are short (3 readings by default) so you do not generate dozens of
alert emails. See the guide for how to de-duplicate alerts in production.
"""
import argparse
import json
import random
import time
from pathlib import Path


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


class Patient:
    """Slowly drifting baseline vitals plus optional abnormal episodes."""

    def __init__(self, scenario, episode_every_s, episode_len, first_episode_s, seed=None):
        self.rng = random.Random(seed)
        self.scenario = scenario
        self.episode_every_s = episode_every_s
        self.episode_len = episode_len
        self.t0 = time.time()
        self.next_episode_at = first_episode_s
        self.episode_left = 0
        self.episode_kind = None
        self.spo2 = 97.0
        self.sys = 120.0
        self.dia = 78.0
        self.hr = 74.0

    def _start_episode_if_due(self, elapsed):
        if self.scenario == "normal" or self.episode_left > 0:
            return
        if elapsed >= self.next_episode_at:
            kinds = {"hypoxia": ["spo2"], "hypertension": ["bp"], "mixed": ["spo2", "bp"]}[self.scenario]
            self.episode_kind = self.rng.choice(kinds)
            self.episode_left = self.episode_len
            self.next_episode_at = elapsed + self.episode_every_s

    def reading(self, device_id):
        elapsed = time.time() - self.t0
        # baseline random walk (stays in a healthy band)
        self.spo2 = clamp(self.spo2 + self.rng.uniform(-0.4, 0.4), 94, 99)
        self.sys = clamp(self.sys + self.rng.uniform(-2, 2), 105, 135)
        self.dia = clamp(self.dia + self.rng.uniform(-1.5, 1.5), 65, 88)
        self.hr = clamp(self.hr + self.rng.uniform(-2, 2), 60, 95)

        spo2, sys_, dia, hr = self.spo2, self.sys, self.dia, self.hr
        self._start_episode_if_due(elapsed)
        if self.episode_left > 0:
            self.episode_left -= 1
            if self.episode_kind == "spo2":
                spo2 = self.rng.uniform(78, 84.5)
                hr = hr + self.rng.uniform(15, 30)
            else:
                sys_ = self.rng.uniform(180, 195)
                dia = self.rng.uniform(105, 120)

        return {
            "deviceId": device_id,
            "ts": int(time.time() * 1000),
            "spo2": round(spo2, 1),
            "systolic": int(round(sys_)),
            "diastolic": int(round(dia)),
            "heartRate": int(round(hr)),
        }


def would_alert(r):
    """Same rule the AWS IoT rule applies - used here only to label console output."""
    reasons = []
    if r["spo2"] < 85:
        reasons.append("LOW_SPO2")
    if r["systolic"] >= 180:
        reasons.append("HIGH_BP")
    return reasons


def make_publisher(args):
    topic = f"medical/{args.device_id}/vitals"

    if args.mode == "print":
        return topic, lambda payload: None, lambda: None

    if args.mode == "boto3":
        import boto3  # noqa: WPS433 (imported lazily on purpose)
        iot = boto3.client("iot", region_name=args.region)
        endpoint = iot.describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
        data = boto3.client("iot-data", region_name=args.region, endpoint_url=f"https://{endpoint}")

        def pub(payload):
            data.publish(topic=topic, qos=1, payload=payload)

        return topic, pub, lambda: None

    # default: real MQTT with certificates
    from awscrt import mqtt
    from awsiot import mqtt_connection_builder

    certs = Path(args.certs)
    conn = mqtt_connection_builder.mtls_from_path(
        endpoint=args.endpoint,
        cert_filepath=str(certs / "device.pem.crt"),
        pri_key_filepath=str(certs / "private.pem.key"),
        ca_filepath=str(certs / "AmazonRootCA1.pem"),
        client_id=args.device_id,          # must match the IoT policy (client/<deviceId>)
        clean_session=True,
        keep_alive_secs=30,
    )
    conn.connect().result(timeout=20)
    print(f"Connected to {args.endpoint} as {args.device_id}")

    def pub(payload):
        fut, _ = conn.publish(topic=topic, payload=payload, qos=mqtt.QoS.AT_LEAST_ONCE)
        fut.result(timeout=10)

    return topic, pub, lambda: conn.disconnect().result(timeout=10)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["mqtt", "boto3", "print"], default="mqtt")
    ap.add_argument("--device-id", default="bp-oximeter-001")
    ap.add_argument("--region", default="eu-west-2")
    ap.add_argument("--endpoint", help="AWS IoT data endpoint (mqtt mode). setup_aws.py prints it.")
    ap.add_argument("--certs", default="certs", help="folder with device.pem.crt, private.pem.key, AmazonRootCA1.pem")
    ap.add_argument("--scenario", choices=["normal", "hypoxia", "hypertension", "mixed"], default="mixed")
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between readings")
    ap.add_argument("--first-episode-s", type=float, default=30.0, help="seconds before first abnormal episode")
    ap.add_argument("--episode-every-s", type=float, default=300.0, help="seconds between abnormal episodes")
    ap.add_argument("--episode-len", type=int, default=3, help="readings per abnormal episode")
    ap.add_argument("--duration-min", type=float, default=30.0, help="stop after N minutes (keeps cost bounded)")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    if args.mode == "mqtt" and not args.endpoint:
        ap.error("--endpoint is required in mqtt mode (setup_aws.py prints it)")

    patient = Patient(args.scenario, args.episode_every_s, args.episode_len, args.first_episode_s, args.seed)
    topic, publish, close = make_publisher(args)
    deadline = time.time() + args.duration_min * 60
    n = 0
    print(f"Publishing to {topic} every {args.interval}s for {args.duration_min} min. Ctrl+C to stop.")
    try:
        while time.time() < deadline:
            r = patient.reading(args.device_id)
            publish(json.dumps(r))
            n += 1
            flag = would_alert(r)
            print(f"#{n:04d} spo2={r['spo2']:>5} bp={r['systolic']}/{r['diastolic']} hr={r['heartRate']}"
                  + (f"   <-- ALERT {','.join(flag)}" if flag else ""))
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        close()
        print(f"Sent {n} messages.")


if __name__ == "__main__":
    main()
