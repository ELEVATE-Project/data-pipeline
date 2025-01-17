#!/bin/bash

set -e

# Function to remove Java
uninstall_java() {
    echo "Checking installed Java versions..."
    java_packages=$(dpkg -l | grep -i 'openjdk\|java' | awk '{print $2}')

    if [[ -n "$java_packages" ]]; then
        echo "Uninstalling Java packages: $java_packages"
        sudo apt purge -y $java_packages
        sudo apt autoremove -y
        sudo apt autoclean
        echo "Java uninstalled successfully."
    else
        echo "No Java packages found to uninstall."
    fi

    # Verify removal
    if java -version >/dev/null 2>&1; then
        echo "Java is still installed. Please check manually."
    else
        echo "Java has been completely removed."
    fi
}


# Function to remove Scala
uninstall_scala() {
    echo "Uninstalling Scala 2.12.11..."
    sudo rm -rf /usr/local/scala-2.12.11 /usr/bin/scala /usr/bin/scalac
    echo "Scala uninstalled successfully."
}

# Function to remove PostgreSQL
uninstall_postgresql() {
    echo "Uninstalling PostgreSQL..."
    sudo apt remove --purge -y postgresql postgresql-contrib
    sudo apt autoremove -y
    sudo rm -rf /var/lib/postgresql /etc/postgresql /etc/postgresql-common /var/log/postgresql
    echo "PostgreSQL uninstalled successfully."
}

# Function to remove Docker
uninstall_docker() {
    echo "Uninstalling Docker..."
    sudo apt remove --purge -y docker.io
    sudo apt autoremove -y
    sudo rm -rf /var/lib/docker /etc/docker
    sudo groupdel docker || true
    echo "Docker uninstalled successfully."
}

# Function to remove Kafka
uninstall_kafka() {
    echo "Uninstalling Kafka..."
    sudo systemctl stop kafka zookeeper
    sudo systemctl disable kafka zookeeper
    sudo rm -rf /opt/kafka /etc/systemd/system/kafka.service /etc/systemd/system/zookeeper.service
    sudo systemctl daemon-reload
    echo "Kafka uninstalled successfully."
}

# Function to remove Flink
uninstall_flink() {
    echo "Uninstalling Flink..."
    sudo rm -rf /opt/flink
    echo "Flink uninstalled successfully."
}

# Function to remove Maven
uninstall_maven() {
    echo "Uninstalling Maven..."
    sudo rm -rf /opt/maven /etc/profile.d/maven.sh
    echo "Maven uninstalled successfully."
}

# Display menu
display_uninstall_menu() {
    echo "Please select a component to uninstall:"
    options=("Uninstall Java" "Uninstall Scala" "Uninstall PostgreSQL" "Uninstall Docker" "Uninstall Kafka" "Uninstall Flink" "Uninstall Maven" "Exit")
    for i in "${!options[@]}"; do
        echo "$((i+1)). ${options[i]}"
    done
}

# Main script
while true; do
    display_uninstall_menu
    read -p "Enter your choice (1-${#options[@]}): " choice
    case $choice in
        1) uninstall_java ;;
        2) uninstall_scala ;;
        3) uninstall_postgresql ;;
        4) uninstall_docker ;;
        5) uninstall_kafka ;;
        6) uninstall_flink ;;
        7) uninstall_maven ;;
        8) echo "Exiting the uninstallation script."; break ;;
        *) echo "Invalid option. Please try again." ;;
    esac
    echo "Operation completed."
done

echo "Uninstallation script completed."
