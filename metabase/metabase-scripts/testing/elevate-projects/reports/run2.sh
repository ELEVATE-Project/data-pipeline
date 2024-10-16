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
first_dir_processed=false
report_name="$1"
parameter_value="$2"

# Loop through subdirectories & Execute the script
for dir in "$target_dir"/*; do
  if [[ -d "$dir" ]]; then
    echo -e "${PINK} Started Processing Directory: $dir ${NC}"
    echo ""
    cd "$dir"
    echo "report_path = $(pwd)"
    report_path=$(pwd)

    # # Run 01_create_dashboard.sh for the first directory
    # if [ "$first_dir_processed" = false ]; then
    #   $main_dir_path/01_create_dashboard.sh "$report_path" "$main_dir_path" "$report_name" 
    #   first_dir_processed=true
    #   if [ "$report_name" == "State-Report" ]; then
    #       echo "Running state-Report.sh"
    #       $main_dir_path/State-Report.sh "$report_path" "$main_dir_path" "$parameter_value"
    #   elif [ "$report_name" == "Program-Report" ]; then
    #       echo "Running program_report.sh"
    #       $main_dir_path/Program-Report.sh "$report_path" "$main_dir_path" "$parameter_value"
    #   elif [ "$report_name" == "Super-Admin-Report" ]; then
    #       echo "Running Super-admin-report.sh"
    #       $main_dir_path/Super-Admin-Report.sh "$report_path" "$main_dir_path" 
    #   else
    #       echo "Invalid report name. Valid options are: State-Report, Program-Report, Super-Admin-Report."
    #       exit 1
    #   fi
    # else
    #   # Run 03_update_json_files.sh for all other directories
    #   $main_dir_path/03_update_json_files.sh "$report_path" "$main_dir_path"
    #   echo ""
    # fi

    # Move back to the parent directory 
    cd ..
    echo "main_dir_path = $(pwd)"
    echo ""
    echo -e "${PINK} Successfully Processed Directory: $dir ${NC}"
    echo ""
  fi
done

echo "Finished processing directories in $target_dir"