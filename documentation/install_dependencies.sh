#!/bin/bash

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to install Java
install_java() {
    echo "Installing Java..."
    sudo apt update
    sudo DEBIAN_FRONTEND=noninteractive apt install -y openjdk-11-jdk
    echo "Java installed successfully."
}

# Function to install Scala
install_scala() {
    if ! command_exists scala || [[ "$(scala -version 2>&1)" != *"2.12.11"* ]]; then
        echo "Installing Scala 2.12.11..."
        curl -O https://downloads.lightbend.com/scala/2.12.11/scala-2.12.11.tgz
        sudo tar -xvzf scala-2.12.11.tgz -C /usr/local
        sudo ln -sf /usr/local/scala-2.12.11/bin/scala /usr/bin/scala
        sudo ln -sf /usr/local/scala-2.12.11/bin/scalac /usr/bin/scalac
        rm scala-2.12.11.tgz
        echo "Scala 2.12.11 installed successfully."
    else
        echo "Scala 2.12.11 is already installed."
    fi
}

install_curl () {
    if ! command_exists curl; then
        echo "Installing curl..."
        sudo apt update
        sudo DEBIAN_FRONTEND=noninteractive apt install -y curl
        echo "curl installed successfully."
    else
        echo "curl is already installed."
    fi



# Function to install PostgreSQL
install_postgresql() {
    echo "Installing PostgreSQL..."
    sudo apt update
    sudo DEBIAN_FRONTEND=noninteractive apt install -y postgresql postgresql-contrib
    echo "PostgreSQL installed successfully."
}

# Function to install Docker
install_docker() {
    echo "Installing Docker..."
    sudo apt update
    sudo DEBIAN_FRONTEND=noninteractive apt install -y docker.io
    sudo systemctl start docker
    sudo systemctl enable docker
    echo "Docker installed successfully."
}

# Function to install Kafka
install_kafka() {
    echo "Installing Kafka..."
    sudo DEBIAN_FRONTEND=noninteractive apt install -y openjdk-11-jdk
    sudo wget https://archive.apache.org/dist/kafka/3.5.0/kafka_2.12-3.5.0.tgz -O kafka.tgz
    sudo tar xzf kafka.tgz -C /opt
    sudo mv /opt/kafka_2.12-3.5.0 /opt/kafka

    echo "Creating Zookeeper service..."
    sudo bash -c 'cat > /etc/systemd/system/zookeeper.service << EOF
    [Unit]
    Description=Apache Zookeeper service
    Documentation=http://zookeeper.apache.org
    Requires=network.target remote-fs.target
    After=network.target remote-fs.target

    [Service]
    Type=simple
    ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
    ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
    Restart=on-abnormal

    [Install]
    WantedBy=multi-user.target
    EOF'

    echo "Creating Kafka service..."
    sudo bash -c 'cat > /etc/systemd/system/kafka.service << EOF
    [Unit]
    Description=Apache Kafka Service
    Documentation=http://kafka.apache.org/documentation.html
    Requires=zookeeper.service

    [Service]
    Type=simple
    Environment="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"
    ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
    ExecStop=/opt/kafka/bin/kafka-server-stop.sh

    [Install]
    WantedBy=multi-user.target
    EOF'

    sudo systemctl daemon-reload
    sudo systemctl start zookeeper
    sudo systemctl start kafka
    echo "Kafka installed successfully."
}

# Function to install Flink
install_flink() {
    echo "Installing Flink..."
    sudo wget https://archive.apache.org/dist/flink/flink-1.16.0/flink-1.16.0-bin-scala_2.12.tgz -O flink.tgz
    sudo tar xzf flink.tgz -C /opt
    sudo mv /opt/flink-1.16.0 /opt/flink
    rm flink.tgz
    echo "Flink installed successfully."
    echo "Starting Flink cluster..."
    sudo ./opt/flink/bin/start-cluster.sh
}

#!/bin/bash

set -e

install_maven() {
    echo "Installing Maven..."

    # Define Maven version and download URL
    MAVEN_VERSION=3.8.8
    MAVEN_URL="https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"

    # Download Maven tarball
    echo "Downloading Maven ${MAVEN_VERSION}..."
    wget ${MAVEN_URL} -O maven.tar.gz

    # Extract Maven tarball
    echo "Extracting Maven..."
    sudo tar xzf maven.tar.gz -C /opt

    # Rename Maven directory
    sudo mv /opt/apache-maven-${MAVEN_VERSION} /opt/maven

    # Clean up tarball
    rm maven.tar.gz

    # Add Maven to PATH
    echo "Configuring environment variables..."
    sudo tee /etc/profile.d/maven.sh > /dev/null <<EOL
    export M2_HOME=/opt/maven
    export PATH=\$M2_HOME/bin:\$PATH
    EOL

    # Make the script executable and load it
    sudo chmod +x /etc/profile.d/maven.sh
    source /etc/profile.d/maven.sh

    # Verify Maven installation
    echo "Verifying Maven installation..."
    mvn -version

    echo "Maven installed successfully!"
}


# Display menu
display_menu() {
    echo "Please select an installation option:"
    options=("Install Java" "Install Scala" "install_maven" "Install PostgreSQL" "Install Docker" "Install Kafka" "Install Flink" "Exit")
    for i in "${!options[@]}"; do
        echo "$((i+1)). ${options[i]}"
    done
}

# Main script
while true; do
    display_menu
    read -p "Enter your choice (1-${#options[@]}): " choice
    case $choice in
        1) install_java ;;
        2) install_scala ;;
        3) install_maven ;;
        4) install_postgresql ;;
        5) install_docker ;;
        6) install_kafka ;;
        7) install_flink ;;
        8) echo "Exiting the installation script."; break ;;
        *) echo "Invalid option. Please try again." ;;
    esac
    echo "Operation completed."
done

echo "Installation script completed."