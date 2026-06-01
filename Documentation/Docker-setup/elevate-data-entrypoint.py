import os
import sys
import time
import base64
import logging
import subprocess
import requests
import xml.etree.ElementTree as ET
from pyhocon import ConfigFactory
from logging.handlers import TimedRotatingFileHandler

UNIFIED_CONF = "/app/unified-common.conf"

class ElevateSupervisor:
    def __init__(self):
        if not os.path.exists(UNIFIED_CONF):
            sys.exit(f"ERROR: Configuration file not found at {UNIFIED_CONF}")
            
        self.conf = ConfigFactory.parse_file(UNIFIED_CONF)
        self.logger = self._setup_logging()
        
        # Load configs
        self.check_interval = int(self.conf.get("health.check.interval.sec"))
        self.flink_url = self.conf.get("flink.url")
        self.job_jars = dict(self.conf.get("health.check.flink.job.jars"))
        
        # Akka
        self.akka_jar = self.conf.get("akka.service.jar")
        self.akka_host = self.conf.get("akka.http.host")
        self.akka_port = int(self.conf.get("akka.http.port"))
        self.akka_health_url = f"http://{self.akka_host}:{self.akka_port}/health"
        self.flink_health_api = f"http://{self.akka_host}:{self.akka_port}/api/health/flink"
        
        # Scripts
        self.cleanup_enabled = self.conf.get("data.cleanup.enabled")
        self.cleanup_script = self.conf.get("data.cleanup.script.path")
        self.mentoring_enabled = self.conf.get("mentoring.batch.job.enabled")
        self.mentoring_script = self.conf.get("mentoring.batch.job.script.path")
        self.mentoring_cron = self.conf.get("mentoring.batch.job.cron.time")
        
        self.session = requests.Session()
        self.session.headers.update({"Authorization": str(self.conf.get("akka.security.api.token", ""))})

    def _setup_logging(self):
        logger = logging.getLogger("elevate-data-entrypoint")
        logger.setLevel(logging.INFO)
        if logger.handlers: return logger
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        try:
            log_path = self.conf.get("elevate.data.entrypoint.log.path")
            fh = TimedRotatingFileHandler(log_path, when="midnight", interval=1, backupCount=7)
            fh.namer = lambda x: f"{x.rsplit('.', 1)[0]}-{x.rsplit('.', 1)[1]}.log"
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        except Exception as e:
            print(f"Warning: Could not setup file logging: {e}")
            
        return logger

    def safe_req(self, method, url, **kwargs):
        """Wrapper for safe requests without breaking supervisor loop."""
        try:
            return getattr(self.session, method)(url, timeout=60, **kwargs)
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout (60s) occurred during {method.upper()} request to {url}")
            return None
        except requests.exceptions.ConnectionError:
            self.logger.error(f"Connection error during {method.upper()} request to {url}. Is the service up?")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error during {method.upper()} request to {url}: {str(e)}")
            return None

    def manage_akka(self):
        try:
            r = requests.get(self.akka_health_url, timeout=5)
        except Exception as e:
            self.logger.warning(f"Akka health check connection failed: {str(e)}")
            r = None
            
        if r and r.status_code == 200 and r.json().get("status") == "UP":
            return
            
        if r and r.status_code != 200:
            self.logger.warning(f"Akka health check returned non-200 status: {r.status_code} - {r.text}")
            
        if not self.akka_jar or not os.path.exists(self.akka_jar):
            return self.logger.error(f"Akka JAR not found on disk: {self.akka_jar}. Path might be misconfigured.")

        self.logger.info(f"Starting akka-service from {self.akka_jar}...")
        try:
            log_file = open(self.conf.get("akka.service.log.path"), "a")
            subprocess.Popen(["java", f"-Dconfig.file={UNIFIED_CONF}", "-jar", self.akka_jar],
                             stdout=log_file, stderr=subprocess.STDOUT, preexec_fn=os.setpgrp)
        except Exception as e:
            self.logger.error(f"Failed to start akka service: {e}")

    def manage_tmux(self, name, script, enabled):
        if not enabled or not script or not os.path.exists(script): return
        self.logger.info(f"tmux is enabled and script is present")
        if subprocess.run(["tmux", "-V"], capture_output=True).returncode != 0:
            self.logger.error("tmux is not installed or not found in PATH!")
            return

        if subprocess.run(["tmux", "has-session", "-t", name], capture_output=True).returncode != 0:
            self.logger.info(f"Starting {name} in tmux...")
            subprocess.run(["tmux", "new-session", "-d", "-s", name, f"python3 {script}"], preexec_fn=os.setsid)

    def manage_cron(self, script, cron, enabled):
        if not enabled or not script or not os.path.exists(script): return
        
        # In Docker, the cron daemon isn't running by default just because it's installed!
        subprocess.run(["service", "cron", "start"], capture_output=True)
        
        res = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        current = res.stdout if res.returncode == 0 else ""
        
        # Dynamically support python execution internally
        interpreter = "python3" if str(script).endswith(".py") else "/bin/bash"
        
        if script not in current:
            new_cron = f"{current.strip()}\n{cron} {interpreter} {script} >> /app/logs/cron-raw-errors.log 2>&1\n".lstrip()
            if subprocess.run(["crontab", "-"], input=new_cron, text=True).returncode == 0:
                self.logger.info(f"Setup cron for {script} using {interpreter}")

    def submit_job(self, jar_path):
        if not os.path.exists(jar_path): 
            return self.logger.error(f"Jar not found: {jar_path}")
        
        # 1. Parse Entry Class
        pom_path = os.path.join(os.path.dirname(os.path.dirname(jar_path)), "pom.xml")
        entry_class = None
        if os.path.exists(pom_path):
            try:
                root = ET.parse(pom_path).getroot()
                ns = root.tag.split("}")[0].lstrip("{") if "}" in root.tag else ""
                match = root.find(f".//{{{ns}}}mainClass" if ns else ".//mainClass")
                entry_class = match.text.strip() if match is not None else None
            except Exception: pass
            
        if not entry_class: 
            return self.logger.error(f"No mainClass found for {jar_path}")

        # 2. Upload/Find Jar on Flink
        jar_name = os.path.basename(jar_path)
        r = self.safe_req("get", f"{self.flink_url}/jars")
        
        jar_id = next((j.get("id", "").split("/")[-1] for j in (r.json().get("files", []) if r else []) 
                       if j.get("name", "").endswith(jar_name)), None)

        if not jar_id:
            with open(jar_path, "rb") as f:
                self.logger.info(f"Uploading jar {jar_name} to Flink API...")
                start_time = time.time()
                r = self.safe_req("post", f"{self.flink_url}/jars/upload", files={"jarfile": f})
                if r:
                    elapsed = round(time.time() - start_time, 2)
                    if r.status_code == 200:
                        jar_id = r.json().get("filename", "").split("/")[-1]
                        self.logger.info(f"Successfully uploaded {jar_name} in {elapsed}s. Jar ID: {jar_id}")
                    else:
                        self.logger.error(f"Failed to upload {jar_name}. Status: {r.status_code}, Response: {r.text}")
                else:
                    self.logger.error(f"Failed to upload {jar_name} due to network or timeout error.")
                    jar_id = None

        if not jar_id: 
            return self.logger.error(f"Aborting job submission for {jar_name}; valid jar_id could not be established.")

        # 3. Submit
        # We pass the mounted config file path to Flink
        args = "--config.file.path /opt/flink/conf/unified-common.conf"
            
        try:
            self.logger.info(f"Attempting to run jar_id: {jar_id} for {jar_name} with entry class {entry_class}...")
            r = self.session.post(f"{self.flink_url}/jars/{jar_id}/run", 
                                  json={"entryClass": entry_class, "programArgs": args},
                                  timeout=30)
            if r.status_code == 200:
                self.logger.info(f"Successfully submitted {jar_name}: {r.status_code} - {r.text}")
            else:
                self.logger.error(f"Failed to submit {jar_name}. Flink API returned Status: {r.status_code}, Response: {r.text}")
        except Exception as e:
            self.logger.error(f"Exception occurred while submitting {jar_name}: {e}")

    def check_jobs_running(self):
        """Ping the API to get all currently running Flink jobs."""
        r = self.safe_req("get", self.flink_health_api)
        if not r:
            self.logger.warning(f"Failed to reach Flink health API at {self.flink_health_api}. Assuming no jobs are running.")
            return {}
        if r.status_code != 200:
            self.logger.warning(f"Flink health API returned Status: {r.status_code}, Response: {r.text}")
            return {}
        return {j.get("name"): j.get("status") == "RUNNING" for j in r.json().get("jobs", [])}

    def start(self):
        self.logger.info("Starting Elevate entry point supervisor...")
        while True:
            # Re-verify and maintain services
            self.manage_akka()
            self.manage_tmux("resource_cleanup", self.cleanup_script, self.cleanup_enabled)
            self.manage_cron(self.mentoring_script, self.mentoring_cron, self.mentoring_enabled)

            # Re-verify and maintain Flink jobs
            running_jobs = self.check_jobs_running()
            self.logger.info(f"Running jobs: {running_jobs}")
            for name, jar in self.job_jars.items():
                if running_jobs.get(name) is True:
                    self.logger.info(f"Job '{name}' is RUNNING.")
                else:
                    self.logger.info(f"Job '{name}' not running. Submitting...")
                    self.submit_job(jar)

            self.logger.info(f"Sleeping {self.check_interval}s...\n")
            time.sleep(self.check_interval)

if __name__ == "__main__":
    ElevateSupervisor().start()
