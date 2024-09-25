#!/bin/bash

# ANSI escape codes for colors
YELLOW="\033[0;33m"
GREEN="\033[0;32m"
NC="\033[0m" # No Color (reset to default)

# Target directory
target_dir="elevate-projects"


# Loop through subdirectories
for dir in "$target_dir"/*; do
  # Check if it's actually a directory
  if [[ -d "$dir" ]]; then
    echo -e "${YELLOW} Started Processing Directory: $dir ${NC}"

    # Change directory to the subdirectory
    cd "$dir"

    # Execute the script (assuming it's in the current directory)
    ./01_create_dashboard.sh 

    # Move back to the parent directory 
    cd - &> /dev/null || true  

    echo ""
    echo -e "${GREEN} Successfully Processed Directory: $dir ${NC}"
    echo ""
  fi
done

echo "Finished processing directories in $target_dir"