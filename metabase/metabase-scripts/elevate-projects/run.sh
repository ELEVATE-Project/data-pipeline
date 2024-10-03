#!/bin/bash

# ANSI escape codes for colors
PINK="\033[0;35m"
GREEN="\033[0;32m"
NC="\033[0m" # No Color (reset to default)

# Color codes for terminal output
BOLD_CYAN="\033[1;36m"
BOLD_YELLOW="\033[1;33m"

echo -e "${BOLD_CYAN}"
echo -e "███    ███ ███████ ████████  █████  ██████   █████  ███████ ███████ "
echo -e "████  ████ ██         ██    ██   ██ ██   ██ ██   ██ ██      ██      "
echo -e "██ ████ ██ █████      ██    ███████ ██████  ███████ ███████ █████   "
echo -e "██  ██  ██ ██         ██    ██   ██ ██   ██ ██   ██      ██ ██      "
echo -e "██      ██ ███████    ██    ██   ██ ██████  ██   ██ ███████ ███████ "
echo -e "${NC}"
# Target directory
target_dir="."
main_dir_path=$(pwd)
# Loop through subdirectories
for dir in "$target_dir"/*; do
  # Check if it's actually a directory
  if [[ -d "$dir" ]]; then
    echo -e "${PINK} Started Processing Directory: $dir ${NC}"
    echo ""
    # Change directory to the subdirectory
    cd "$dir"
    report_path=$(pwd)
    # Execute the script (assuming it's in the current directory)
    $main_dir_path/01_create_dashboard.sh $report_path

    # Move back to the parent directory 
    cd - &> /dev/null || true  

    echo ""
    echo -e "${PINK} Successfully Processed Directory: $dir ${NC}"
    echo ""
  fi
done

echo "Finished processing directories in $target_dir"