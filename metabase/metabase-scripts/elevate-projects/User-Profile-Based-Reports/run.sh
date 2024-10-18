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
parameter_value1="$2"
parameter_value2="$3"
# exec >> /home/user1/Documents/elevate-metabase/elevate-data-pipeline/metabase/metabase-scripts/User-Profile-Based-Reports/logs/State-Report.log 2>&1
# Parameter validation based on report type
if [[ "$report_name" == "State-Report" ]]; then
    if [[ -z "$parameter_value1" ]]; then
        echo "Error: State name is required as parameter 1 for the State-Report."
        echo "Ex. ./run.sh "State-Report"  "Karnataka""
        exit 1
    fi
elif [[ "$report_name" == "District-Report" ]]; then
    if [[ -z "$parameter_value1" || -z "$parameter_value2" ]]; then
        echo "Error: Both state name (parameter 1) and district name (parameter 2) are required for the District-Report."
        echo "Ex : ./run.sh "District-Report"  "Andhra Pradesh" "Vijayawada""
        exit 1
    fi
elif [[ "$report_name" == "Program-Report" ]]; then
    if [[ -z "$parameter_value1" ]]; then
        echo "Error: Program name is required as parameter 1 for the Program-Report."
        echo "Ex : ./run.sh "Program-Report" "DCPCR School Development Index 2018-19" "
        exit 1
    fi
elif [[ "$report_name" == "Super-Admin-Report" ]]; then
    if [[ -n "$parameter_value1" || -n "$parameter_value2" ]]; then
        echo "Error: No parameters should be provided for the Super-Admin-Report."
        echo "Ex : ./run.sh "Super-Admin-Report" "
        exit 1
    fi
else
    echo "Invalid report name. Valid options are: State-Report, District-Report, Program-Report, Super-Admin-Report."
    exit 1
fi

# Loop through subdirectories & Execute the script
for dir in "$target_dir"/*; do
  if [[ -d "$dir" ]]; then
    echo -e "${PINK} Started Processing Directory: $dir ${NC}"
    echo ""
    cd "$dir"
    report_path=$(pwd)
    # Run 01_create_dashboard.sh for the first directory
    if [ "$first_dir_processed" = false ]; then
      $main_dir_path/01_create_dashboard.sh "$report_path" "$main_dir_path" "$report_name" "$parameter_value1" "$parameter_value2"
      first_dir_processed=true
      if [ "$report_name" == "State-Report" ]; then
          echo "Running State-Report.sh"
          $main_dir_path/State-Report.sh "$report_path" "$main_dir_path" "$parameter_value1"
      elif [ "$report_name" == "District-Report" ]; then
          echo "Running District-Report.sh"
          $main_dir_path/District-Report.sh "$report_path" "$main_dir_path" "$parameter_value1" "$parameter_value2"
      elif [ "$report_name" == "Program-Report" ]; then
          echo "Running Program-Report.sh"
          $main_dir_path/Program-Report.sh "$report_path" "$main_dir_path" "$parameter_value1"
      elif [ "$report_name" == "Super-Admin-Report" ]; then
          echo "Running Super-Admin-Report.sh"
          $main_dir_path/Super-Admin-Report.sh "$report_path" "$main_dir_path" 
      else
          echo "Invalid report name. Valid options are: State-Report, Program-Report, Super-Admin-Report."
          exit 1
      fi
    else
      # Run 03_update_json_files.sh for all other directories
      $main_dir_path/03_update_json_files.sh "$report_path" "$main_dir_path"
      echo ""
    fi

    # Move back to the parent directory 
    cd ..
    echo ""
    echo -e "${PINK} Successfully Processed Directory: $dir ${NC}"
    echo ""
  fi
done

echo "Finished processing directories"