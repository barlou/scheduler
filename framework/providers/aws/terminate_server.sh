#!/bin/bash
# framework/providers/aws/terminate_server.sh
# ─────────────────────────────────────────────────────────────────────────────
# Terminate an EC2 instance by ID.
#
# Usage:
#   ./terminate_server.sh --instance-id i-0abc123 --region eu-west-1
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

INSTANCE_ID=""
REGION="eu-west-1"

while [[ $# -gt 0 ]]; do
    case "$1" in 
        --instance-id) INSTANCE_ID="$2"; shift 2 ;;
        --region)      REGION="$2";      shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$INSTANCE_ID" ]]; then
    echo "::error::Missing required argument: --instance-id"
    exit 1
fi

echo "Terminating: $INSTANCE_ID in $REGION"

aws ec2 terminate-instances \
    --region       "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --output       text > /dev/null

echo "Waiting for termination..."
aws ec2 wait instance-terminated \
    --region       "$REGION" \
    --instance-ids "$INSTANCE_ID"

echo "Terminated: $INSTANCE_ID"