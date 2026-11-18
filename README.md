# CrimeTrends Analytics

## Cambridge Police Daily Log --- Data Engineering Pipeline & Analytics Dashboard

> **Project:** CrimeTrends Analytics\
> **Data source:** Cambridge Police Department Daily Police Log (Socrata
> dataset `3gki-wyrb`)\
> **Pipeline:** Socrata API → Python/Pandas ETL → PostgreSQL → Metabase\
> **Orchestration:** Prefect\
> **Cloud deployment:** AWS EC2 (Ubuntu 24.04, Docker Compose)

This README documents the **CrimeTrends project implementation**: Python
ETL, PostgreSQL, Prefect, and Metabase.

------------------------------------------------------------------------

## 1. Project overview

This project collects public police-log records from Cambridge,
Massachusetts [Daily Police Log](https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb)
(Socrata dataset `3gki-wyrb`), validates and transforms them, loads them into
PostgreSQL, and presents analytical views in Metabase.

### High-level architecture

``` text
Cambridge Police Daily Police Log (Socrata API)
                     |
                     v
             Python extraction
                     |
                     v
       Validation + transformation
       (Pandas / Python utilities)
                     |
                     v
          PostgreSQL (cpd_db)
                     |
                     v
              Metabase
          Interactive dashboard

              Prefect
       (orchestrates ETL runs
        and daily schedule)
```

### What the pipeline does

1.  **Extract:** retrieves the Cambridge Police Daily Police Log from
    the Socrata API.
2.  **Validate:** checks required fields, missing IDs, and duplicate
    records.
3.  **Transform:** standardizes and prepares records for analytics,
    including date/time-derived fields.
4.  **Load:** writes records to PostgreSQL using an upsert process.
5.  **Visualize:** Metabase reads the PostgreSQL table and displays
    KPIs, trends, incident types, temporal patterns, and locations.
6.  **Orchestrate:** Prefect supports manual runs and a daily scheduled
    run.

**Important interpretation note:** these are police-log entries, not
necessarily confirmed crimes. Counts describe entries in the source
dataset; they do not establish crime rates, causality, or individual
risk.

------------------------------------------------------------------------

## 2. Repository structure

The exact project may contain additional notebooks, SQL files, and
helper modules. The core files used by the pipeline are:

``` text
CrimeTrends/
├── src/
│   ├── __init__.py
│   ├── etl_pipeline.py       # Prefect ETL flow and deployment serving
│   ├── extract.py            # Socrata API extraction
│   ├── validate.py           # Data quality checks
│   ├── transform.py           # Data cleaning and feature derivation
│   └── load.py                # PostgreSQL loading / upsert
├── tests/                     # If present: project tests
├── docker-compose.yml         # Local development services
├── docker-compose.aws.yml     # AWS deployment services
├── requirements.txt           # Python dependencies
├── .env.example               # Environment variable template (if present)
├── .gitignore
└── README.md
```

Do not commit `.env`, database passwords, API tokens, private keys,
database dumps, or virtual-environment folders.

------------------------------------------------------------------------

## 3. Prerequisites for a Windows local demo

Install these before starting:

-   **Git for Windows**
-   **Python 3.11 or a compatible version used by the project**
-   **Docker Desktop** (with the WSL 2 backend enabled)
-   A working internet connection

After installing Docker Desktop, open it and wait until Docker reports
that its engine is running.

Check installations in **Windows PowerShell**:

``` powershell
git --version
python --version
docker --version
docker compose version
```

If `python` is not recognized, try:

``` powershell
py --version
```

------------------------------------------------------------------------

## 4. Get the project using Git

Open **Windows PowerShell** and move to the folder where you keep
projects:

``` powershell
cd "$HOME\Desktop"
```

Clone your project's actual GitHub repository:

``` powershell
git clone https://github.com/anmolbuilds/Crime_Trends_Dashboard_DE-Project.git
cd CrimeTrends
```

Replace the placeholder with the repository URL that contains the `src/`
folder and the project's Docker Compose files. If the repository uses a
different folder name, `cd` into that folder.

If you already have the project locally, skip `git clone` and `cd` into
that existing project folder.

Check that the expected files are present:

``` powershell
Get-ChildItem
Get-ChildItem .\src
```

------------------------------------------------------------------------

## 5. Configure environment variables

The project uses environment variables for API access and database
credentials. Never paste real secrets into GitHub or a public README.

If the repository contains `.env.example`, create your local `.env`:

``` powershell
Copy-Item .env.example .env
notepad .env
```

Fill in the values expected by the project. The variables used in the
current deployment include:

``` dotenv
SOCRATA_APP_TOKEN=your_socrata_app_token

POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=choose_a_strong_local_password

PGADMIN_DEFAULT_EMAIL=you@example.com
PGADMIN_DEFAULT_PASSWORD=choose_a_pgadmin_password

METABASE_DB_USER=your_metabase_db_user
METABASE_DB_PASSWORD=choose_a_metabase_db_password
```

Use the exact variable names in your repository's Compose file and
Python code. If a variable is not referenced by the local Compose
configuration, it may not be needed for the local demo.

If `.env.example` does not exist, inspect `docker-compose.yml` and the
code for `os.getenv(...)` / `os.environ[...]` references before creating
`.env`. Do not invent variable names.

Confirm `.env` is ignored by Git:

``` powershell
git check-ignore .env
```

If this command prints nothing, check `.gitignore` and add `.env` before
proceeding.

------------------------------------------------------------------------

## 6. Create a Python virtual environment

From the project root in **PowerShell**:

``` powershell
py -3 -m venv venv
.\venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, run this once in the same PowerShell
window:

``` powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1
```

Upgrade pip and install dependencies:

``` powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

The terminal prompt should show `(venv)` when the environment is active.

If the project imports `psycopg` but it is not included in
`requirements.txt`, install the driver used by the project. For the
current deployment, the binary package was installed with:

``` powershell
pip install "psycopg[binary]"
```

If the source code imports `psycopg2` instead, install the matching
`psycopg2-binary` dependency rather than changing imports blindly.

------------------------------------------------------------------------

## 7. Start the local database and dashboard services

Make sure Docker Desktop is running. In **PowerShell**, from the project
root:

``` powershell
docker compose up -d
docker compose ps
```

This starts the services defined in the **local** `docker-compose.yml`.
In the current project setup these include PostgreSQL for incident data,
Metabase, and the Metabase application database; pgAdmin may also be
included in the local Compose file.

Useful local endpoints (confirm the actual port mappings in
`docker-compose.yml`):

  Service                  Typical local address
  ------------------------ -------------------------
  Metabase                 `http://localhost:3000`
  pgAdmin (if included)    `http://localhost:8081`
  PostgreSQL incident DB   `localhost:5433`

Follow container logs if a service is still starting:

``` powershell
docker compose logs -f
```

To inspect one service:

``` powershell
docker compose logs -f postgres_cpd
docker compose logs -f metabase
```

Press `Ctrl+C` to stop following logs; this does **not** stop the
containers.

------------------------------------------------------------------------

## 8. Run the ETL pipeline locally

Open a **second PowerShell window** (or reuse the first after the
containers are ready).

Navigate to the project folder and activate the virtual environment:

``` powershell
cd "<PATH_TO_YOUR_PROJECT>\CrimeTrends"
.\venv\Scripts\Activate.ps1
```

Run the ETL flow directly:

``` powershell
python -c "from src.etl_pipeline import etl; etl()"
```

The pipeline should extract the source records, run validation and
transformation, and load the resulting records into PostgreSQL.

### Expected validation evidence

In the successful cloud run, the pipeline processed:

-   Extracted records: **12,845**
-   Unique IDs: **12,845**
-   Duplicate records: **0**
-   Missing IDs: **0**
-   Validated records: **12,845**
-   Rejected records: **0**
-   Transformed records: **12,845**

These are the observed figures from the earlier successful run; the live
source dataset may change, so your local run can produce different
counts.

### Verify the database load

The incident database is `cpd_db`, and the main table used in the
deployment is `cpd_incidents`.

If the local Compose service is named `postgres_cpd` and maps host port
`5433` to container port `5432`, a typical check is:

``` powershell
docker exec -it postgres_cpd_dev psql -U <POSTGRES_USER> -d cpd_db -c "SELECT COUNT(*) FROM cpd_incidents;"
```

Replace `<POSTGRES_USER>` with the local `POSTGRES_USER` value from
`.env`. If the container name differs, get it using:

``` powershell
docker compose ps
```

------------------------------------------------------------------------

## 9. Open Metabase and connect the data

Open:

``` text
http://localhost:3000
```

If this is the first local launch, complete Metabase's initial setup.
Add a PostgreSQL database connection using the values from your
**local** Compose configuration:

  -----------------------------------------------------------------------
  Setting                             Value
  ----------------------------------- -----------------------------------
  Database type                       PostgreSQL

  Host                                `postgres_cpd` (Docker service
                                      hostname, if that is the service
                                      name)

  Port                                `5432`

  Database name                       `cpd_db`

  Username                            Value of `POSTGRES_USER`

  Password                            Value of `POSTGRES_PASSWORD`

  SSL                                 Off for the local Docker network,
                                      unless configured otherwise
  -----------------------------------------------------------------------

Use the Docker service hostname when Metabase is itself running in
Docker. Do not use `localhost` from inside the Metabase container to
reach the PostgreSQL container.

After saving the connection, let Metabase sync the schema. Open the
saved dashboard if it has been imported/restored, or create questions
from the `cpd_incidents` table.

------------------------------------------------------------------------

## 10. Optional: run Prefect locally

The ETL flow can be run directly as shown above. To demonstrate
orchestration locally, start a Prefect server and register the flow as a
served deployment.

### Terminal 1 --- start the Prefect server

``` powershell
cd "<PATH_TO_YOUR_PROJECT>\CrimeTrends"
.\venv\Scripts\Activate.ps1
prefect server start --host 127.0.0.1
```

Keep this terminal open. The local UI is:

``` text
http://127.0.0.1:4200
```

### Terminal 2 --- point Prefect to the local server

``` powershell
cd "<PATH_TO_YOUR_PROJECT>\CrimeTrends"
.\venv\Scripts\Activate.ps1
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Then start the flow-serving process:

``` powershell
python -m src.etl_pipeline
```

The flow should report that it is being served and polling for scheduled
runs. Open the Prefect UI, navigate to Deployments, and use the Run
control to trigger a manual run.

**Schedule note:** the deployment code in the cloud setup was configured
for a daily midnight schedule in `Asia/Kolkata`. Confirm that the local
`src/etl_pipeline.py` contains that same schedule before claiming the
local schedule is configured. A local Prefect server and serving process
must remain running for local orchestration.

To stop the local processes, press `Ctrl+C` in the flow-serving terminal
and then in the Prefect server terminal.

------------------------------------------------------------------------

## 11. Useful commands

Run from the project root in PowerShell.

  -------------------------------------------------------------------------------------------
  Task                                Command
  ----------------------------------- -------------------------------------------------------
  Start containers                    `docker compose up -d`

  List containers                     `docker compose ps`

  Follow all logs                     `docker compose logs -f`

  Follow ETL/container logs (if       `docker compose logs -f etl`
  service exists)                     

  Stop containers, preserve database  `docker compose down`
  volumes                             

  Rebuild images                      `docker compose up --build -d`

  Activate Python environment         `.\venv\Scripts\Activate.ps1`

  Install dependencies                `pip install -r requirements.txt`

  Run ETL once                        `python -c "from src.etl_pipeline import etl; etl()"`

  Check Prefect version               `prefect version`
  -------------------------------------------------------------------------------------------

**Do not run `docker compose down -v`** unless you intentionally want to
delete the named database volumes and their data.

------------------------------------------------------------------------

## 12. Troubleshooting

### Docker command fails or containers are unavailable

-   Start Docker Desktop and wait until its engine is ready.
-   Run `docker compose ps`.
-   Inspect logs with `docker compose logs -f`.

### PostgreSQL connection refused

-   Confirm the PostgreSQL container is running and healthy.
-   Check the host port mapping in `docker-compose.yml`.
-   For a connection from Metabase container to PostgreSQL, use the
    Compose service hostname and container port `5432`.
-   For a connection from a Python process running directly on Windows,
    use the published host port (typically `5433`).

### Missing Python module

-   Confirm `(venv)` appears in the terminal.
-   Run `python -m pip install -r requirements.txt`.
-   Install only the missing dependency that matches the import used by
    the source code.

### ETL cannot connect to PostgreSQL

-   Confirm the database container is ready.
-   Check `.env` credentials and the host/port expected by
    `src/load.py`.
-   Note that the current project loader connects through the
    host-published PostgreSQL port when Python runs directly on the
    host. If you run the ETL inside Docker, its connection settings must
    instead use the database service hostname and container port.

### Prefect UI opens but no deployment appears

-   Confirm the Prefect server is running.
-   Confirm `PREFECT_API_URL` points to `http://127.0.0.1:4200/api`.
-   Confirm the flow-serving terminal is still running and reports that
    it is polling for runs.

------------------------------------------------------------------------

## 13. Cloud deployment: AWS EC2

The cloud deployment is separate from your laptop's local POC.

Current deployment details:

  -----------------------------------------------------------------------
  Component                           Configuration
  ----------------------------------- -----------------------------------
  Cloud provider                      AWS

  Region                              Sydney (`ap-southeast-2`)

  EC2 instance                        `CrimeTrends-Server`

  Instance type                       `t3.small`

  Operating system                    Ubuntu 24.04 LTS

  Public IPv4                         `16.179.147.124`

  Application directory               `/home/ubuntu/CrimeTrends`

  Containers                          PostgreSQL, Metabase, Metabase
                                      application PostgreSQL

  ETL orchestration                   Prefect server + flow-serving
                                      systemd services

  Metabase port                       `3000`

  Prefect API/UI                      Bound to `127.0.0.1:4200`, accessed
                                      using SSH tunnel
  -----------------------------------------------------------------------

### Cloud URLs

Metabase application:

``` text
http://16.179.147.124:3000
```

A Metabase **public sharing link**, if enabled and generated in
Metabase, is a different URL. Keep in mind that anyone with a public
link may be able to view the shared dashboard. Do not expose sensitive
data.

Prefect UI is intentionally bound to the EC2 loopback interface and is
not directly public. From Windows PowerShell, open an SSH tunnel:

``` powershell
ssh -i "<PATH_TO_KEY>\crimetrends-key.pem" -L 4200:127.0.0.1:4200 ubuntu@16.179.147.124
```

Keep that SSH window open, then browse:

``` text
http://localhost:4200
```

### What happens if the laptop is shut down?

**Yes --- the cloud Metabase URL should continue working after you shut
down your laptop**, provided that:

-   the EC2 instance remains running (do not stop or terminate it);
-   the Docker containers and systemd services remain healthy;
-   AWS networking/security-group rules remain in place; and
-   the public IP address has not changed.

The ETL schedule runs on EC2, not on your laptop. Your laptop and SSH
session are not required for the scheduled ETL to continue.

The Prefect UI tunnel is different: if you shut down your laptop or
close the SSH tunnel, `http://localhost:4200` on your laptop will no
longer reach the cloud Prefect UI. The Prefect server and flow-serving
service continue running on EC2.

------------------------------------------------------------------------

## 14. How to present the project

A clear demonstration sequence:

1.  **Problem and source:** explain that the project analyzes the
    Cambridge Police Daily Police Log from the public Socrata API.
2.  **Architecture:** walk through API → Extract → Validate → Transform
    → PostgreSQL → Metabase, with Prefect orchestrating the ETL.
3.  **ETL code:** show `extract.py`, `validate.py`, `transform.py`,
    `load.py`, and `etl_pipeline.py`.
4.  **Data quality:** show extracted/validated/rejected counts,
    missing-ID checks, and duplicate handling.
5.  **Database:** show the `cpd_incidents` table and run a simple
    `COUNT(*)` query.
6.  **Dashboard:** demonstrate the KPI cards, incident trends,
    incident-type breakdown, temporal analysis, location analysis, and
    record table.
7.  **Orchestration:** show the Prefect deployment, a manual Run, and
    the daily schedule configured for midnight Asia/Kolkata.
8.  **Cloud deployment:** explain that Docker services and Prefect run
    on EC2, while Metabase is reachable through the public EC2 endpoint.
9.  **Resilience:** explain that systemd keeps Prefect services running
    and Docker restart policies keep containers running after a restart.

### Technical concepts to be ready to explain

-   **ETL:** Extract, Transform, Load.
-   **API pagination:** retrieving a dataset in batches rather than
    assuming one response contains every record.
-   **Data validation:** enforcing required fields and checking data
    quality before loading.
-   **Deduplication:** keeping one record per incident ID.
-   **Upsert:** insert new records and update existing records without
    blindly duplicating them.
-   **PostgreSQL:** relational database used to persist the transformed
    incident data.
-   **Docker:** packages services into containers for repeatable
    deployment.
-   **Docker Compose:** defines and runs the multi-container
    application.
-   **Prefect:** orchestrates flow execution, manual runs, deployments,
    and schedules.
-   **Metabase:** BI/analytics layer that queries PostgreSQL and
    visualizes the data.
-   **EC2:** virtual machine hosting the cloud deployment.
-   **systemd:** Linux service manager used to keep Prefect server and
    flow-serving processes running.
-   **Environment variables:** configuration/secrets kept outside source
    code.

### Questions your evaluator may ask

**Why use PostgreSQL?**\
It provides persistent relational storage and supports SQL queries and
BI tools such as Metabase.

**Why use Prefect?**\
It manages and observes ETL execution, supports manual triggering, and
schedules recurring runs.

**Why Docker Compose?**\
It makes the multi-service stack reproducible and simplifies starting,
stopping, and inspecting services.

**What happens if the ETL runs again?**\
The load uses an upsert strategy keyed by incident ID, so reruns can
update existing records rather than simply appending duplicates.

**Does a new ETL run guarantee new incidents?**\
No. It retrieves what the source API currently publishes. If the city
has not published newer entries, the pipeline cannot create them.

**Are all records confirmed crimes?**\
No. They are entries in a police daily log and may include different
kinds of police activity.

**Why is Prefect not publicly exposed?**\
The Prefect UI/API is bound to localhost and accessed through an SSH
tunnel to reduce public exposure.

------------------------------------------------------------------------

## 15. Final pre-demo checklist

### Local POC

-   [ ] Git clone or local project folder is ready.
-   [ ] `.env` exists locally and is not committed.
-   [ ] Python virtual environment is active.
-   [ ] `pip install -r requirements.txt` completed.
-   [ ] Docker Desktop is running.
-   [ ] `docker compose up -d` completed.
-   [ ] PostgreSQL container is healthy.
-   [ ] ETL completes successfully.
-   [ ] PostgreSQL row count is verified.
-   [ ] Metabase opens and displays the data.
-   [ ] Prefect UI/deployment works, if demonstrating orchestration
    locally.

### Cloud POC

-   [ ] EC2 instance is running.
-   [ ] `prefect-server` systemd service is active.
-   [ ] `prefect-worker` systemd service is active.
-   [ ] Docker containers are running.
-   [ ] Metabase URL opens.
-   [ ] Dashboard charts display data.
-   [ ] Prefect UI opens through the SSH tunnel.
-   [ ] Manual Prefect run completes.
-   [ ] Daily schedule is visible in the deployment configuration.

### Security and cost

-   [ ] Never commit `.env` or the `.pem` key.
-   [ ] Keep PostgreSQL ports private.
-   [ ] Keep Prefect bound to localhost unless a secure access method is
    deliberately configured.
-   [ ] Restrict SSH to your current public IP.
-   [ ] Stop or terminate AWS resources when no longer needed; a running
    EC2 instance can continue to incur charges.

------------------------------------------------------------------------

## 16. Notes

-   This guide documents the current CrimeTrends implementation and its
    observed deployment configuration. If local Compose service names or
    ports differ in your checked-out branch, trust the actual
    `docker-compose.yml` and `.env.example` in that repository.
-   The local POC and cloud POC are separate environments. Running the
    local ETL does not update the cloud database, and running the cloud
    ETL does not update the local database.
-   Keep the project repository free of credentials, database backups,
    private keys, and local virtual environments.
