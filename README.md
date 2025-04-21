<div align="center">

# Report Service

<a href="https://shikshalokam.org/elevate/">
<img
    src="https://shikshalokam.org/wp-content/uploads/2021/06/elevate-logo.png"
    height="140"
    width="300"
  />
</a>

![GitHub package.json version (subfolder of monorepo)](https://img.shields.io/github/package-json/v/ELEVATE-Project/mentoring?filename=src%2Fpackage.json)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

</details>
</details>

</br>
The Reports building block facilitates the creation and engagement with dashboards.

</div>
</br>

# Supported Operating Systems
-   **Ubuntu (Recommended: Version 22.04)**

# Setup Options

> **Note:** This guide focuses on setting up services using Docker. For a streamlined and efficient setup, it is recommended to use Dockerized Services & Dependencies with the provided Docker-Compose file.

<details><summary>Dockerized Services & Dependencies Using Docker-Compose File</summary>

## Dockerized Services & Dependencies

## Expectation
By diligently following the outlined steps, you will successfully establish a fully operational data service application setup.

## Prerequisites

### **Setting Up `curl`, `git`, and `netstat` on Ubuntu**

<details><summary> 1. Install curl </summary>
`curl` is used for making HTTP requests from the command line.

🔹 **Check if `curl` is installed:**
```bash
  curl --version
```  
🔹 **If not installed, install it using:**
```bash
  sudo apt update && sudo apt install -y curl
```  
</details>

<details><summary> 2. Install git </summary>
`git` is required for cloning repositories and managing version control.

🔹 **Check if `git` is installed:**
```bash
  git --version
```  
🔹 **If not installed, install it using:**
```bash
  sudo apt update && sudo apt install -y git
```  
</details>

<details><summary> 3. Install netstat (via `net-tools`) </summary>
`netstat` is used to check network connections and ports.

🔹 **Check if `netstat` is installed:**
```bash
  netstat -tulnp
```  
🔹 **If not installed, install it using:**
```bash
  sudo apt update && sudo apt install -y net-tools
```  
</details>

To set up the data service application, ensure you have Docker and Docker Compose installed on your system. For Ubuntu users, detailed installation instructions for Docker found here : [Install Docker engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/) and for docker-compose follow this documentation: [How To Install and Use Docker Compose on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04).
## Installation

**Create report Directory:** Establish a directory titled **reports**.

> Example Command: `mkdir reports && cd reports/`

> Note: All commands are run from the reports directory.

> **Caution:** Before proceeding, please ensure that the ports given here are available and open. It is essential to verify their availability prior to moving forward. You can run the below command in your terminal to check this:

```bash
   for port in 3000 2181 9092 8081 5432 5050 9092 8080; do
       if sudo lsof -iTCP:$port -sTCP:LISTEN &>/dev/null || sudo netstat -tulnp | grep -w ":$port" &>/dev/null; then
           echo "Port $port is in use"
       else
           echo "Port $port is available"
       fi
   done
```

### Steps to Set Up Dockerized Services

1. **Download and execute the main setup script:** Execute the following command in your terminal from the reports directory.
 ```bash
   curl -OJL https://raw.githubusercontent.com/ELEVATE-Project/data-pipeline/main/Documentation/Docker-setup/setup.sh && chmod +x setup.sh && sudo ./setup.sh
 ```

   > **Note:** The script will download the necessary files and launch the services in Docker. Once all services are up and running, follow the provided steps and enter the required inputs when prompted by the script. For instructions on setting up PgAdmin and Metabase, please refer to the documentation.
   
2. **General Instructions:**
    - All containers which are part of the docker-compose can be gracefully stopped by pressing `Ctrl + C` in the same terminal where the services are running.
    - To start all services and dependencies:
        ```bash
        sudo docker compose --env-file ./config.env up -d
        ```
    - To stop all containers and remove volumes:
       ```bash
       sudo ./docker-compose down -v
       ```

## Setting Up pgAdmin [optional]
<details><summary>Set Up pgAdmin </summary>

1. Open pgAdmin by navigating to `http://localhost:5050` in your browser.
2. Log in with the default credentials:
    - **Username:** `admin@example.com`
    - **Password:** `admin`
3. Click on **Servers** → **Create** → **Server**.
4. Under the **General** tab, provide a name for your server (e.g., `PostgresServer`).
5. Under the **Connection** tab:
    - **Host:** `postgres`
    - **Port:** `5432`
    - **Maintenance Database:** `dev-project-analytics`
    - **Username:** `postgres`
    - **Password:** `password`
6. Click **Save** to connect to the PostgreSQL database.
</details>

## Setting Up Metabase
<details><summary>Configure Metabase </summary>

1. Open Metabase by navigating to `http://localhost:3000` in your browser.
   ![Here is the opening page of Metabase Dashboard](/Documentation/Docker-setup/screenshots/01.png)
2. Select the Preferred Language:
   **Choose language:** `English` -> **Next**
   ![Select the Preferred Language](/Documentation/Docker-setup/screenshots/02.png)
3. Setup super admin login credentials
   - **Set First Name** `elevate`
   - **Set Last Name** `user`
   - **set Company or team name** `shikshalokam`
   - **Set Email** `user@shikshalokam.org`
   - **Set Password** `elevate@123` -> **Next** \ 

    ![setup super admin login credentials](/Documentation/Docker-setup/screenshots/03.png)
4. Setup the database connection.
     - **Set up your first database** → Select **PostgreSQL** 

   ![Select the postgres database to connect)](/Documentation/Docker-setup/screenshots/04.png)
     - then click on **show more options**
5. Enter the database credentials
     - **Display name:** `elevateData`
     - **Host:** `postgres`
     - **Port:** `5432`
     - **Database name:** `dev-project-analytics`
     - **Username:** `postgres`
     - **Password:** `password`

     ![Add the configuration as per mentioned in the config.env](/Documentation/Docker-setup/screenshots/05.png)
        
3. Click **Take me to Metabase** to complete the setup and start using Metabase.
   ![Then you all set to go](/Documentation/Docker-setup/screenshots/07.png)
4. Setting up the `state_name` and `district_name` data type in the metabase.
    - Go to setting → Admin setting → Table Metadata → select the projects table
    - Set the data type 'state' for state_name and 'city' for district_name.
    - This settings will save automatically. 
   
   Here is the screen shot for setting up the state_name semantic type.
   
   ![change the type of state_name](/Documentation/Docker-setup/screenshots/08.png)
   Here is the screen shot for setting up the district_name semantic type.
   ![change the type of district_name](/Documentation/Docker-setup/screenshots/09.png)
</details>

## Onboarding user managemennt
<details><summary>Onboarding user management </summary>

Here is the csv for checking the other reports.  
[user_data.csv](Documentation/Docker-setup/user_data.csv) .
Also you can interact with the api to onboard new users.
```bash
   curl --location 'http://localhost:8080/api/csv/upload' \
   --header 'Authorization: 4a2d9f8e-3b56-47c1-a9d3-e571b8f0c2d9' \
   --form 'file=@"/app/Documentation/Docker-setup/user_data.csv"' 
```
To list out the users
```bash
   curl --location 'http://localhost:8080/api/csv/list' \
   --header 'Authorization: 4a2d9f8e-3b56-47c1-a9d3-e571b8f0c2d9' 
```
</details>

</details>

# Team

<a href="https://github.com/ELEVATE-Project/data-pipeline/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=ELEVATE-Project/data-pipeline" />
</a>

# Open Source Dependencies

Several open source dependencies that have aided data-pipeline and dashboards development:

<p>
  <img src="https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka" />
  <img src="https://img.shields.io/badge/Apache%20Flink-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white" />
  <img src="https://img.shields.io/badge/Scala-DC322F?style=for-the-badge&logo=scala&logoColor=white" />
  <img src="https://img.shields.io/badge/Akka-0052CC?style=for-the-badge&logo=akka&logoColor=white" />
  <img src="https://img.shields.io/badge/Metabase-509EE3?style=for-the-badge&logo=metabase&logoColor=white" />
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white" />
</p>
