#!/bin/bash
#
# Supervisor service monitor — sends an email when a service is not RUNNING.
# Only alerts once per incident (uses a state file to track).
#
# Usage: run from cron every 2-5 minutes
#   */3 * * * * /home/your-ssh-user/ledger/monitor/check-services.sh

ALERT_EMAIL="alerts@example.com"
STATE_DIR="$HOME/ledger/monitor/state"
HOSTNAME=$(hostname)

mkdir -p "$STATE_DIR"

# Get all supervised processes and their statuses
supervisorctl status 2>/dev/null | while read -r name status rest; do
  state_file="$STATE_DIR/$name"

  if [ "$status" != "RUNNING" ]; then
    # Service is down — alert if we haven't already
    if [ ! -f "$state_file" ]; then
      echo "$status" > "$state_file"
      mail -s "[$HOSTNAME] $name is $status" "$ALERT_EMAIL" <<EOF
Service: $name
Status:  $status $rest
Host:    $HOSTNAME
Time:    $(date '+%Y-%m-%d %H:%M:%S')

Check with: supervisorctl status
Logs:    supervisorctl tail $name stderr
EOF
      echo "$(date) ALERT: $name is $status — email sent" >> "$STATE_DIR/../monitor.log"
    fi
  else
    # Service is running — clear any previous alert
    if [ -f "$state_file" ]; then
      rm "$state_file"
      mail -s "[$HOSTNAME] $name is RECOVERED" "$ALERT_EMAIL" <<EOF
Service: $name
Status:  RUNNING (recovered)
Host:    $HOSTNAME
Time:    $(date '+%Y-%m-%d %H:%M:%S')
EOF
      echo "$(date) RECOVERED: $name is running again — email sent" >> "$STATE_DIR/../monitor.log"
    fi
  fi
done
