#!/bin/bash 
# framework/providers/aws/setup_server.sh
# ─────────────────────────────────────────────────────────────────────────────
# Launch an EC2 instance and print the instance ID to stdout.
# Called by external tooling — the Python provider.py is preferred
# for all Airflow framework operations.
#
# Usage:
#   ./setup_server.sh \
#     --instance-type t3.large \
#     --ami-id ami-xxxxxxxx \
#     --subnet-id subnet-xxxxxxxx \
#     --security-group-id sg-xxxxxxxx \
#     --iam-profile airflow-job-runner \
#     --region eu-west-1
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

INSTANCE_TYPE=""
AMI_ID=""
SUBNET_ID=""
SECURITY_GROUP_ID=""
IAM_PROFILE=""
REGION="eu-west-1"

while [[ $# -gt 0 ]]; do
    case "$1" in 
        --instance-type)     INSTANCE_TYPE="$2";     shift 2 ;;
        --ami-id)            AMI_ID="$2";            shift 2 ;;
        --subnet-id)         SUBNET_ID="$2";         shift 2 ;;
        --security-group-id) SECURITY_GROUP_ID="$2"; shift 2 ;;
        --iam-profile)       IAM_PROFILE="$2";       shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# Validate required args 
for var in INSTANCE_TYPE AMI_ID SUBNET_ID SECURITY_GROUP_ID IAM_PROFILE; do
    if [[ -z "${!var}" ]]; then 
        echo "::error::Missing required argument: --$(echo $var | tr '_' '-' | tr '[:upper:]' '[:lower:]')"
        exit 1
    fi
done

echo "Launching EC2 instances..."
echo "  Type:   $INSTANCE_TYPE"
echo "  AMI:    $AMI_ID"
echo "  Region: $REGION"

INSTANCE_ID=$(aws ec2 run-instances \
    --region        "$REGION" \
    --image-id      "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --subnet-id     "$SUBNET_ID" \
    --security-group-ids "$SECURITY_GROUP_ID" \
    --iam-instance-profile Name="$IAM_PROFILE" \
    --tag-specification \
        "ResourceType=instance,Tags=[{Key=ManagedBy,Value=airflow-framework}]" \
    --metadata-options \ 
        "HttpTokens=required, HttpEndpoint=enabled" \
    --query "Instances[0].InstanceId" \
    --output text)

echo "Launched: $INSTANCE_ID"

echo "Waiting for instance to pass status check..."
aws ec2 wait instance-status-ok \
    --region       "$REGION" \
    --instance-ids "$INSTANCE_ID"

echo "Ready: $INSTANCE_ID"
echo "$INSTANCE_ID"