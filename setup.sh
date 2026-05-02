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
#   SSM_REGION          e.g. eu-west-3                  (required if SECRETS_BACKEND=ssm)
#   DEPLOYMENTS_BASE    e.g. /home/ubuntu/deployments
#   SECRETS_BACKEND     ssm (default) | github          (required if SECRETS_BACKEND=ssm)
#
# Github backend - values injectes as env vars by _deploy.yml:
#   AIRFLOW_DB_HOST, AIRFLOW_DB_PORT, AIRFLOW_DB_NAME, AIRFLOW_DB_USER,
#   AIRFLOW_DB_PASSWORD, AIRFLOW_SECRET_KEY, AIRFLOW_ADMIN_PASSWORD,
#   AIRFLOW_SMTP_HOST, AIRFLOW_SMTP_PORT, AIRFLOW_SMTP_FROM, AIRFLOW_REDIS_HOST,
#
# Design:
#   - SECRETS_BACKEND selects the resolver (fetch_secret dispatcher)
#   - All callers use fetch_secret() — they don't know which backend runs
#   - Adding a new backend (e.g, azure-keyvault) only required adding a case
#     block to fetch_secret() and writing it in _deploy.yml
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# Variable initialisation
AIRFLOW_ID="${1:-airflow-prod-1}"
ENVIRONMENT="${2:-production}"
DEPLOYMENTS_BASE="${DEPLOYMENTS_BASE:-$HOME/deployments}"
AIRFLOW_HOME="${DEPLOYMENTS_BASE}/airflow"
AIRFLOW_PORT="${AIRFLOW_PORT:-8080}"
TIMEZONE="${TIMEZONE:-Europe/Paris}"
PARALLELISM="${PARALLELISM:-8}"
MAX_ACTIVE_TASKS="${MAX_ACTIVE_TASKS:-4}"
AIRFLOW_VERSION="2.9.0"
PYTHON_BIN="python3"

# Secrets backend — ssm is the default to keep backward compatibility 
SECRETS_BACKEND="${SECRETS_BACKEND:-ssm}"

LOG_FILE="$DEPLOYMENTS_BASE/airflow/logs/setup.log"
mkdir -p "$DEPLOYMENTS_BASE/airflow/logs"

# Tee all output to log file
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=== Airflow setup starting ==="
echo "  Airflow ID      : $AIRFLOW_ID"
echo "  Environment     : $ENVIRONMENT"
echo "  Airflow home    : $AIRFLOW_HOME"
echo "  Deployments     : $DEPLOYMENTS_BASE"
echo "  Port            : $AIRFLOW_PORT"
echo "  Secrets backend : $SECRETS_BACKEND"

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

# fetch_secret <smm_path_suffix> <env_var_name> [default value]
#
# Resolves a single secret regardless of the backend in use
#
# - ssm backend    : calls AWS SSM using the suffix under $SSM_PREFIX
# - github backend : reads the value from an env var already present in the 
#                    process environment (injected by _deploy.yml secrets block)
# - env backend    : reads from the same env var — useful for local dev/testing 
#                    via a sourced .env file 
#
# Args:
#   $1  SSM suffix      e.g, "db/host"        -> /$SSM_REFIX/db/host
#   $2  Env var name    e.g, "AIRFLOW_B_HOST" -> read from $AIRFLOW_DB_HOST
#   $3  Default value   (optional, empty string by default)
#
# Returns the resolved value on stdout. Exits 1 if the value is empty and no 
# default aws provided (backend=ssm) or the env var is missing (backend=github)

fetch_secret() {
    local ssm_suffix="$1"
    local env_var_name="${2}"
    local default="${3:-}"
    local value=""

    case "${SECRETS_BACKEND}" in 

        # —— AWS SSM 
        ssm)
            local full_path="/${SSM_PREFIX%/}${ssm_suffix}"
            value=$(aws ssm get-parameter \
                --name            "$full_path" \
                --with-decryption \
                --region          "${SSM_REGION:-eu-west-3}" \
                --query           "Parameter.Value" \
                --output          text \
                2>/dev/null) || value="$default"
            if [[ -z "$value" && -z "$default" ]]; then
                echo "::error::SSM parameter missing or empty; $full_path" >&2
                echo "  Run: python3 ssm_preflight.py --env $ENVIRONMENT" >&2
                exit 1
            fi
            ;;
        # —— Github Actions Secrets
        github)
            value="${!env_var_name:-}"

            if [[ -z "$value" && -z "$default" ]]; then 
                echo "::error::Github secret env var not set: $env_var_name" >&2
                echo "  Add it to your workflow secrets and map it in _deploy.yml" >&2
                exit 1
            fi
            value="${value:-$default}"
            ;;
        # —— local .env fallback
        env)
            value="${!env_var_name:-$default}"
            if [[ -z "$value" && -z "$default" ]]; then
                echo "::error::Env var not set: $env_var_name (backend=env)" >&2
                echo "  Make sure you sourced your .env file before calling setup.sh"
                exit 1
            fi
            ;;
        *)
            echo "::error::Unknown SECRETS_BACKEND: '$SECRETS_BACKEND'" >&2
            echo "  Supported: ssm | github | env" >&2
            exit 1
            ;;
    esac
        printf '%s' "$value"
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
echo "--- Step 3: Fetch secrets [backend: $SECRETS_BACKEND] ---"

DB_HOST=$(fetch_secret    "/db/host"    "AIRFLOW_DB_HOST")
DB_PORT=$(fetch_secret    "/db/port"    "AIRFLOW_DB_PORT"    "5432")
DB_NAME=$(fetch_secret    "/db/name"    "AIRFLOW_DB_NAME")
DB_USER=$(fetch_secret    "/db/user"    "AIRFLOW_DB_USER")
DB_PASSWORD=$(fetch_secret "/db/password"   "AIRFLOW_DB_PASSWORD")

AIRFLOW_ACCESS_KEY=$(fetch_secret "/airflow/access_key" "AIRFLOW_ACCESS_KEY")
AIRFLOW_SECRET_KEY=$(fetch_secret "/airflow/secret_key" "AIRFLOW_SECRET_KEY")
ADMIN_PASSWORD=$(fetch_secret     "/admin/password"     "ADMIN_PASSWORD")

SMTP_HOST=$(fetch_secret  "/smtp/host"  "AIRFLOW_SMTP_HOST"   "localhost")
SMTP_PORT=$(fetch_secret  "/smtp/port"  "AIRFLOW_SMTP_PORT"   "587")
SMTP_FROM=$(fetch_secret  "/smtp/from"  "AIRFLOW_SMTP_FROM"   "airflow@localhost")

REDIS_HOST=$(fetch_secret "/redis/host" "AIRFLOW_REDIS_HOST"  "localhost")

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
OUTPUT="$DEPLOYMENTS_BASE/airflow/airflow.cfg"

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
# STEP 5 — Export AIRFLOW_ID for dag_factory_loader
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 5: Configure environment ---"

# Write AIRFLOW_HOME and AIRFLOW_ID to a persistent env file
# Sourced by Airflow service on startup
unset AIRFLOW_HOME

AIRFLOW_HOME="$DEPLOYMENTS_BASE/airflow"
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
# STEP 6 — Initialise database if first run
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 6: Database initialisation ---"

AIRFLOW_HOME="$AIRFLOW_HOME" airflow db check 2>/dev/null \
    && DB_EXISTS=true \
    || DB_EXISTS=false

if [[ "$DB_EXISTS" == "false" ]]; then
    echo "  First run — initialising database..."
    AIRFLOW_HOME="$AIRFLOW_HOME" airflow db migrate
    echo "  [OK] database initialised"

    # Create default admin user
    ADMIN_PASSWORD=$(fetch_secret "$SSM_PREFIX/admin/password" "admin")
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
# STEP 7 — Start or reload services
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 7: Start services ---"

unset AIRFLOW_HOME
AIRFLOW_HOME="$DEPLOYMENTS_BASE/airflow"

# Scheduler
if pgrep -f "airflow scheduler" >/dev/null 2>&1; then
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
if AIRFLOW_HOME="$AIRFLOW_HOME" airflow webserver --VERSION &>/dev/null && \
    curl -s connect-timeout 3 "https://localhost:$AIRFLOW_PORT/health" | grep -q "healthy"; then
    echo "  [OK] webserver already running on port $AIRFLOW_PORT"
else
    # Kill any stale webserver process before starting fresh
    pkill -f "airflow webserver" 2>/dev/null || true
    sleep 2

    echo "  Starting webserver on port $AIRFLOW_PORT..."
    AIRFLOW_HOME="$AIRFLOW_HOME" airflow webserver \
        --daemon \
        --port "$AIRFLOW_PORT" \
        --log-file "$DEPLOYMENTS_BASE/airflow/logs/webserver.log" \
        --pid "$AIRFLOW_HOME/webserver.pid"

    # Wait for webserver to be ready
    RETRIES=12
    until curl -s --connect-timeout 3 "http://localhost:$AIRFLOW_PORT/health" \
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