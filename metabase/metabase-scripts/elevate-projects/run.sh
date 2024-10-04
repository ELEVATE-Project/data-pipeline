#!/bin/bash

# ANSI escape codes for colors
PINK="\033[0;35m"
NC="\033[0m" 
BOLD_CYAN="\033[1;36m"

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
# Loop through subdirectories & Execute the script
for dir in "$target_dir"/*; do
  if [[ -d "$dir" ]]; then
    echo -e "${PINK} Started Processing Directory: $dir ${NC}"
    echo ""
    cd "$dir"
    report_path=$(pwd)
    $main_dir_path/01_create_dashboard.sh $report_path

    # Move back to the parent directory 
    cd ..

    echo ""
    echo -e "${PINK} Successfully Processed Directory: $dir ${NC}"
    echo ""
  fi
done

echo "Finished processing directories in $target_dir"