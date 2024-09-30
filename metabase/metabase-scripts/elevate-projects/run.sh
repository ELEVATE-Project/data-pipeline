#!/bin/bash

# ANSI escape codes for colors
YELLOW="\033[0;33m"
GREEN="\033[0;32m"
NC="\033[0m" # No Color (reset to default)

# Target directory
target_dir="."
main_dir=$(pwd)
#to main_dir_path 
# Loop through subdirectories
for dir in "$target_dir"/*; do
  # Check if it's actually a directory
  if [[ -d "$dir" ]]; then
    echo -e "${YELLOW} Started Processing Directory: $dir ${NC}"

    # Change directory to the subdirectory
    cd "$dir"
    inside_dir_path=$(pwd)
    #report_path 
    # Execute the script (assuming it's in the current directory)
    $main_dir/01_create_dashboard.sh $main_dir $inside_dir_path

    # Move back to the parent directory 
    cd - &> /dev/null || true  

    echo ""
    echo -e "${GREEN} Successfully Processed Directory: $dir ${NC}"
    echo ""
  fi
done

echo "Finished processing directories in $target_dir"