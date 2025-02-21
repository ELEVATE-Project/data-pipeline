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
The Reports building block facilitates the creation and engagement with Dshboards.

</div>
</br>

# Setup Options

**Elevate Report services can be set up using two methods:**
> **Note:** This guide outlines two setup methods, detailed below. For a quick, beginner-friendly setup and walkthrough of services, it is recommended to use the **Dockerized Services & Dependencies** setup with the Docker-Compose file.

<details><summary>Dockerized Services & Dependencies Using Docker-Compose File</summary>

## Dockerized Services & Dependencies

### Expectation
By diligently following the outlined steps, you will successfully establish a fully operational data service application setup.

## Prerequisites
To set up the data service application, ensure you have Docker and Docker Compose installed on your system. For Ubuntu users, detailed installation instructions for both can be found in the documentation here: [How To Install and Use Docker Compose on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04).
## Installation

**Create report Directory:** Establish a directory titled **reports**.

> Example Command: `mkdir reports && cd reports/`

> Note: All commands are run from the reports directory.
### Operating Systems: Linux

> **Caution:** Before proceeding, please ensure that the ports given here are available and open. It is essential to verify their availability prior to moving forward. You can run the below command in your terminal to check this:

```
   for port in 3000 2181 9092 8081 5432 5050 9092 5000; do
       if sudo lsof -iTCP:$port -sTCP:LISTEN &>/dev/null || sudo netstat -tulnp | grep -w ":$port" &>/dev/null; then
           echo "Port $port is in use"
       else
           echo "Port $port is available"
       fi
   done
```

### Steps to Set Up Dockerized Services

1. **Download and execute the main setup script:** Execute the following command in your terminal from the reports directory.
    ```
      curl -OJL https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/setup.sh && chmod +x setup.sh && sudo ./setup.sh
    ```

   > **Note:** The script will download all the essential files and launch the services in Docker. Once all services are successfully up and running, Follow the steps and give required inputs to the script.To setup PgAdmin and metabase steps are provide in the documents.
   
2. **General Instructions:**
    - All containers which are part of the docker-compose can be gracefully stopped by pressing `Ctrl + C` in the same terminal where the services are running.
    - To start all services and dependencies:
        ```
        sudo docker compose --env-file ./config.env up -d
        ```
    - To stop all containers and remove volumes:
       ```
       sudo ./docker-compose down -v
       ```

**Keep the current terminal session active, and kindly open a new terminal window within the project directory.**

## Setting Up pgAdmin [optional]

### Step 1: Set Up pgAdmin
1. Open pgAdmin by navigating to `http://localhost:5050` in your browser.
2. Log in with the default credentials:
    - **Username:** `admin@example.com`
    - **Password:** `admin`
3. Click on **Servers** → **Create** → **Server**.
4. Under the **General** tab, provide a name for your server (e.g., `PostgresServer`).
5. Under the **Connection** tab:
    - **Host:** `postgres`
    - **Port:** `5432`
    - **Maintenance Database:** `project-analytics`
    - **Username:** `postgres`
    - **Password:** `password`
6. Click **Save** to connect to the PostgreSQL database.

## Setting Up Metabase

### Step 1: Configure Metabase
1. Open Metabase by navigating to `http://localhost:3000` in your browser.
2. Follow the setup wizard:
    - Setup the Super Admin account.
        - **Choose language:** `English` -> **Next**
        - **Set First Name** `elevate`
        - **Set Last Name** `user`
        - **set Company or team name** `shikshalokam`
        - **Set Email** `user@shikshalokam.org`
        - **Set Password** `elevate@123` -> **Next**
    - Setup the database connection.
        - **Set up your first database** → Select **PostgreSQL**
        - then click on **show more options**
        - Enter the database credentials:
            - **Database connection name:** `elevateData`
            - **Host:** `postgres`
            - **Port:** `5432`
            - **Database name:** `project-analytics`
            - **Username:** `postgres`
            - **Password:** `password`
3. Click **Next** to complete the setup and start using Metabase.
4. setting up the `state_name` and `district_name` data type in the metabase.
    - Go to setting → Admin setting → Table Metadata → select the projects table
    - Set the data type 'state' for state_name and 'city' for district_name.
    - This settings will save automatically.
   1. Here is the opening page of Metabase Dashboard
   ![Here is the opening page of Metabase Dashboard](/Documentation/Docker-setup/screenshots/01.png)
   2. Select the Preferred Language
   ![Select the Preferred Language](/Documentation/Docker-setup/screenshots/02.png)
   3. setup super admin login credentials
   ![setup super admin login credentials](/Documentation/Docker-setup/screenshots/03.png)
   4. Select the postgres database to connect
   ![Select the postgres database to connect)](/Documentation/Docker-setup/screenshots/04.png)
   5. Add the configuration as per mentioned in the config.env
   ![Add the configuration as per mentioned in the config.env](/Documentation/Docker-setup/screenshots/05.png)
   6. Click on the next button
   ![Then you all set to go](/Documentation/Docker-setup/screenshots/07.png)

</details>

