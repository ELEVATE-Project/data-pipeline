#!/bin/bash
source /etc/profile
export TZ=Asia/Kolkata
date

SCRIPT_PATH="/app/Documentation/batch-scripts/mentoring.py"

echo "RUNNING JOB"

echo ""
echo "$(date)"
echo "====================================="
echo "Mentoring Batch script has triggered"

# Correct variable usage with $
python3 "$SCRIPT_PATH"

echo "Mentoring Batch script has finished"
echo "*************************************"