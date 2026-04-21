#!/bin/bash
# setup.sh
# ─────────────────────────────────────────────────────────────────────────────
# Install Airflow, render airflow.cfg, deploy framework, start services.
#
# Called by deploy.sh after barlou/scheduler is copied to the server:
#   bash ~/deploy.sh
#   └── install_airflow() → bash setup.sh "$AIRFLOW_ID" "$ENVIRONMENT"
#
# Args:
#   $1 = AIRFLOW_ID     e.g. airflow-prod-1
#   $2 = ENVIRONMENT    e.g. production | INT | UAT
#
# Env vars expected (set by deploy_env.sh before this script runs):
#   SSM_PREFIX          e.g. /production/airflow
#   DEPLOYMENTS_BASE    e.g. /home/ubuntu/deployments
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

AIRFLOW_ID="${1:-airflow-prod-1}"
ENVIRONMENT="${2:-production}"
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"
DEPLOYMENTS_BASE="${DEPLOYMENTS_BASE:-$HOME/deployments}"
AIRFLOW_PORT="${AIRFLOW_PORT:-8080}"
TIMEZONE="${TIMEZONE:-Europe/Paris}"
PARALLELISM="${PARALLELISM:-8}"
MAX_ACTIVE_TASKS="${MAX_ACTIVE_TASKS:-4}"
AIRFLOW_VERSION="2.9.0"
PYTHON_BIN="python3"

LOG_FILE="$DEPLOYMENTS_BASE/airflow/logs/setup.log"
mkdir -p "$DEPLOYMENTS_BASE/airflow/logs"

# Tee all output to log file
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=== Airflow setup starting ==="
echo "  Airflow ID   : $AIRFLOW_ID"
echo "  Environment  : $ENVIRONMENT"
echo "  Airflow home : $AIRFLOW_HOME"
echo "  Deployments  : $DEPLOYMENTS_BASE"
echo "  Port         : $AIRFLOW_PORT"

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

fetch_ssm() {
    local param="$1"
    local default="${2:-}"
    local value
    value=$(aws ssm get-parameter \
        --name "$param" \
        --with-decryption \
        --query "Parameter.Value" \
        --output text 2>/dev/null) || value="$default"
    echo "$value"
}

service_running() {
    pgrep -f "$1" >/dev/null 2>&1
}

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Install Airflow if not present or version mismatch
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 1: Install Airflow ---"

INSTALLED_VERSION=$(airflow version 2>/dev/null || echo "none")

if [[ "$INSTALLED_VERSION" == *"$AIRFLOW_VERSION"* ]]; then
    echo "  [OK] Airflow $AIRFLOW_VERSION already installed"
else
    echo "  Installing Airflow $AIRFLOW_VERSION..."

    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

    pip install \
        "apache-airflow==${AIRFLOW_VERSION}" \
        apache-airflow-providers-amazon \
        pyyaml \
        croniter \
        boto3 \
        --constraint "$CONSTRAINT_URL" \
        --quiet

    echo "  [OK] Airflow $AIRFLOW_VERSION installed"
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Create directory structure
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 2: Create directories ---"

mkdir -p \
    "$AIRFLOW_HOME/dags" \
    "$AIRFLOW_HOME/logs" \
    "$AIRFLOW_HOME/plugins" \
    "$DEPLOYMENTS_BASE/airflow/logs"

echo "  [OK] directories created"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Fetch secrets from SSM
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 3: Fetch secrets from SSM ($SSM_PREFIX) ---"

DB_HOST=$(fetch_ssm    "$SSM_PREFIX/db/host")
DB_PORT=$(fetch_ssm    "$SSM_PREFIX/db/port"     "5432")
DB_NAME=$(fetch_ssm    "$SSM_PREFIX/db/name")
DB_USER=$(fetch_ssm    "$SSM_PREFIX/db/user")
DB_PASSWORD=$(fetch_ssm "$SSM_PREFIX/db/password")

AIRFLOW_SECRET_KEY=$(fetch_ssm "$SSM_PREFIX/secret_key")

SMTP_HOST=$(fetch_ssm  "$SSM_PREFIX/smtp/host"   "localhost")
SMTP_PORT=$(fetch_ssm  "$SSM_PREFIX/smtp/port"   "587")
SMTP_FROM=$(fetch_ssm  "$SSM_PREFIX/smtp/from"   "airflow@localhost")

REDIS_HOST=$(fetch_ssm "$SSM_PREFIX/redis/host"  "localhost")

# Fetch public hostname from EC2 metadata (IMDSv2)
TOKEN=$(curl -s -X PUT \
    "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" \
    2>/dev/null || echo "")

if [[ -n "$TOKEN" ]]; then
    AIRFLOW_HOST=$(curl -s \
        -H "X-aws-ec2-metadata-token: $TOKEN" \
        "http://169.254.169.254/latest/meta-data/public-hostname" \
        2>/dev/null || hostname -f)
else
    AIRFLOW_HOST=$(hostname -f)
fi

echo "  [OK] secrets fetched"
echo "  Airflow host : $AIRFLOW_HOST"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Render airflow.cfg from template
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 4: Render airflow.cfg ---"

TEMPLATE="$DEPLOYMENTS_BASE/airflow/airflow.cfg.template"
OUTPUT="$AIRFLOW_HOME/airflow.cfg"

if [ ! -f "$TEMPLATE" ]; then
    echo "::error::Template not found: $TEMPLATE"
    exit 1
fi

python3 - <<PYEOF
import re, os

template_path = "$TEMPLATE"
output_path   = "$OUTPUT"

raw = open(template_path).read()

substitutions = {
    "AIRFLOW_HOME":           "$AIRFLOW_HOME",
    "DEPLOYMENTS_BASE":       "$DEPLOYMENTS_BASE",
    "TIMEZONE":               "$TIMEZONE",
    "PARALLELISM":            "$PARALLELISM",
    "MAX_ACTIVE_TASKS_PER_DAG": "$MAX_ACTIVE_TASKS",
    "DB_HOST":                "$DB_HOST",
    "DB_PORT":                "$DB_PORT",
    "DB_NAME":                "$DB_NAME",
    "DB_USER":                "$DB_USER",
    "DB_PASSWORD":            "$DB_PASSWORD",
    "AIRFLOW_HOST":           "$AIRFLOW_HOST",
    "AIRFLOW_PORT":           "$AIRFLOW_PORT",
    "AIRFLOW_SECRET_KEY":     "$AIRFLOW_SECRET_KEY",
    "BUCKET_NAME":            os.environ.get("BUCKET_NAME", ""),
    "SMTP_HOST":              "$SMTP_HOST",
    "SMTP_PORT":              "$SMTP_PORT",
    "SMTP_FROM":              "$SMTP_FROM",
    "REDIS_HOST":             "$REDIS_HOST",
}

for key, value in substitutions.items():
    raw = raw.replace("{{ " + key + " }}", value)
    raw = raw.replace("{{" + key + "}}", value)

with open(output_path, "w") as f:
    f.write(raw)

os.chmod(output_path, 0o600)
print(f"  [OK] rendered: {output_path}")
PYEOF

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Deploy framework and DAG loader
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 5: Deploy framework ---"

SCHEDULER_SRC="$DEPLOYMENTS_BASE/airflow"

# Copy framework to AIRFLOW_HOME
if [ -d "$SCHEDULER_SRC/framework" ]; then
    rm -rf "$AIRFLOW_HOME/framework"
    cp -r "$SCHEDULER_SRC/framework" "$AIRFLOW_HOME/framework"
    echo "  [OK] framework deployed"
else
    echo "::error::framework/ not found at $SCHEDULER_SRC/framework"
    exit 1
fi

# Copy DAG loader
if [ -f "$SCHEDULER_SRC/dags/dag_factory_loader.py" ]; then
    cp "$SCHEDULER_SRC/dags/dag_factory_loader.py" \
       "$AIRFLOW_HOME/dags/dag_factory_loader.py"
    echo "  [OK] dag_factory_loader.py deployed"
else
    echo "::error::dag_factory_loader.py not found"
    exit 1
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Export AIRFLOW_ID for dag_factory_loader
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 6: Configure environment ---"

# Write AIRFLOW_HOME and AIRFLOW_ID to a persistent env file
# Sourced by Airflow service on startup
ENV_FILE="$AIRFLOW_HOME/.airflow_env"

cat > "$ENV_FILE" <<EOF
export AIRFLOW_HOME="$AIRFLOW_HOME"
export AIRFLOW_ID="$AIRFLOW_ID"
export AIRFLOW_DEPLOYMENTS_BASE="$DEPLOYMENTS_BASE"
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME"
EOF

chmod 600 "$ENV_FILE"
source "$ENV_FILE"
echo "  [OK] environment configured at $ENV_FILE"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — Initialise database if first run
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 7: Database initialisation ---"

AIRFLOW_HOME="$AIRFLOW_HOME" airflow db check 2>/dev/null \
    && DB_EXISTS=true \
    || DB_EXISTS=false

if [[ "$DB_EXISTS" == "false" ]]; then
    echo "  First run — initialising database..."
    AIRFLOW_HOME="$AIRFLOW_HOME" airflow db migrate
    echo "  [OK] database initialised"

    # Create default admin user
    ADMIN_PASSWORD=$(fetch_ssm "$SSM_PREFIX/admin/password" "admin")
    AIRFLOW_HOME="$AIRFLOW_HOME" airflow users create \
        --username admin \
        --firstname Airflow \
        --lastname Admin \
        --role Admin \
        --email admin@localhost \
        --password "$ADMIN_PASSWORD" \
        2>/dev/null || true

    echo "  [OK] admin user created"
else
    echo "  [OK] database exists — running migrations..."
    AIRFLOW_HOME="$AIRFLOW_HOME" airflow db migrate
    echo "  [OK] migrations complete"
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — Start or reload services
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 8: Start services ---"

# Scheduler
if service_running "airflow scheduler"; then
    echo "  Scheduler running — triggering DAG reserialization..."
    AIRFLOW_HOME="$AIRFLOW_HOME" airflow dags reserialize 2>/dev/null || true
    echo "  [OK] DAGs reloaded"
else
    echo "  Starting scheduler..."
    AIRFLOW_HOME="$AIRFLOW_HOME" airflow scheduler \
        --daemon \
        --log-file "$DEPLOYMENTS_BASE/airflow/logs/scheduler.log" \
        --pid "$AIRFLOW_HOME/scheduler.pid"
    echo "  [OK] scheduler started"
fi

# Webserver
if service_running "airflow webserver"; then
    echo "  [OK] webserver already running on port $AIRFLOW_PORT"
else
    echo "  Starting webserver on port $AIRFLOW_PORT..."
    AIRFLOW_HOME="$AIRFLOW_HOME" airflow webserver \
        --daemon \
        --port "$AIRFLOW_PORT" \
        --log-file "$DEPLOYMENTS_BASE/airflow/logs/webserver.log" \
        --pid "$AIRFLOW_HOME/webserver.pid"

    # Wait for webserver to be ready
    RETRIES=12
    until curl -s "http://localhost:$AIRFLOW_PORT/health" \
          | grep -q "healthy" || [ "$RETRIES" -eq 0 ]; do
        echo "  Waiting for webserver... ($RETRIES retries left)"
        sleep 5
        RETRIES=$((RETRIES - 1))
    done

    if [ "$RETRIES" -gt 0 ]; then
        echo "  [OK] webserver ready"
    else
        echo "  WARNING: webserver may not be fully ready yet"
        echo "  Check: $DEPLOYMENTS_BASE/airflow/logs/webserver.log"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Airflow setup complete ==="
echo ""
echo "  URL          : http://$AIRFLOW_HOST:$AIRFLOW_PORT"
echo "  Airflow ID   : $AIRFLOW_ID"
echo "  DAGs folder  : $AIRFLOW_HOME/dags"
echo "  Logs         : $DEPLOYMENTS_BASE/airflow/logs"
echo "  Config       : $AIRFLOW_HOME/airflow.cfg"
echo ""
echo "  Next DAG scan in ~30s"
echo "  Monitor : tail -f $DEPLOYMENTS_BASE/airflow/logs/scheduler.log"