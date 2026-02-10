<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://www.i2sc.net">
    <img src="poi_searcher/figures/logo.png" ="Logo" width="800" height="110">
  </a>

  <h3 align="center">Google Places POI Search & Live Monitoring</h3>

</div>

---

## **Table of Contents**
1. [Project Description](#project-description)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
5. [Running the Code](#running-the-code)
6. [Output](#output)
7. [Contributing](#contributing)
8. [Contact](#contact)

---

### Project Description

This monitoring system is designed to identify and monitor Points of Interest (POIs) using Google Maps data. It focuses on extracting dynamic information such as **live busyness**, **popular times histograms**, and **place metadata** at scale.

**Key Features:**
*   **POI Discovery**: Search for places based on keywords, bounding boxes, or city names.
*   **Live Monitoring**: Continuously track "live busyness" levels to detect foot traffic spikes.
*   **Proxy Network Management**: Automatically rotates through a list of proxies to bypass rate limits and distribute traffic.
*   **Secure Authentication**: Uses environment variables for proxy credentials.
*   **Data Pipeline**: Extracts data into structured formats (Parquet/JSON) for analysis.

The system uses a sophisticated scraping pipeline (found in `live_busyness_extraction/` and `api_json_extraction/`) to fetch data directly from Google Maps JSON endpoints, ensuring high throughput and resilience against anti-bot measures.

---

### Project Structure
Only the main **tracked** components (runtime data like `logs/`, `output/`, `uploads/`, etc. are ignored and not listed here):

```
├── api_json_extraction/              # API + extraction service
│   ├── app/
│   │   ├── main.py                   # FastAPI entry point (web app)
│   │   ├── models.py                 # Pydantic / DB models
│   │   ├── dependencies.py           # Auth & common dependencies
│   │   └── services/                 # Background jobs, proxies, pipeline bridge, ...
│   ├── config/
│   │   └── config.py                 # Central configuration (DB, Redis, proxies)
│   ├── db/                           # Database models & connection helpers
│   └── scripts/                      # Helper scripts (e.g. shared busyness cron)
│
├── live_busyness_extraction/         # Live busyness JSON-response pipeline
│   ├── extractor.py                  # Core extractor logic for Google Maps JSON
│   └── main.py                       # Batch runner used by cron / jobs
│
├── poi_searcher/                     # POI discovery pipeline (first stage)
│   ├── src/                          # POI search logic
│   ├── data/
│   │   ├── config.yaml               # POI search configuration
│   │   └── sorted_place_types_list.txt  # Master list of place types (tracked)
│   └── figures/logo.png              # Project logo
│
├── web_app/                          # HTML templates and static assets for the UI
│   ├── templates/                    # Jinja2 templates (search, jobs, admin, auth, ...)
│   └── static/                       # CSS, images, etc.
│
├── README.md                         # This file
├── pyproject.toml                    # Project metadata & dependencies
├── requirements.txt                  # Additional dependency pinning (optional)
└── uv.lock                           # Locked dependency versions for `uv`
```
---

### Prerequisites

1.  **Python 3.8+** (managed via `uv`)
2.  **uv**: This project uses [uv](https://github.com/astral-sh/uv) for dependency management and execution.
3.  **Proxy Credentials**: You must configure your proxy authentication.
    Create a `.env` file in the root directory:
    ```bash
    PROXY_USERNAME=your_username
    PROXY_PASSWORD=your_password
    ```

4.  **Proxy List**: Ensure you have a valid list of proxies. The system expects a `proxies.txt` file (or similar) with one proxy per line in `IP:PORT` format:
    ```text
    192.168.1.10:8080
    10.0.0.5:3128
    172.16.0.23:8000
    ...
    ```

---

### Installation

Clone the repository:
```bash
git clone https://github.com/Societal-Computing/google_maps_poi_search.git
cd google_maps_poi_search
```

Install the required dependencies using `uv`:
```bash
uv sync
```
*This command will create a virtual environment and install all dependencies defined in `pyproject.toml` and `uv.lock`.*

---

### Running the Code

The system is operated via the **web application** (FastAPI + HTML UI). You normally do **not** run individual scripts directly.

#### 1. Start the API server

From the project root:

```bash
bash api_json_extraction/scripts/start_api.sh
```

This will:
- Start the FastAPI app.
- Connect to the local SQLite database in `api_json_extraction/test.db`.
- Connect to Redis using the configuration from `api_json_extraction/config/config.py` and your `.env`.

Make sure:
- Redis is running and reachable at `REDIS_HOST:REDIS_PORT`.
- Your `.env` contains valid `PROXY_USERNAME`, `PROXY_PASSWORD` and (optionally) `PROXY_FILE` pointing to a proxies file.

#### 2. Use the web app

Once the server is running, open your browser at:

- `http://127.0.0.1:8000/` – Home page.

Typical workflow:

1. **Register a user**
   - Go to `http://127.0.0.1:8000/register`
   - Create an account (username + password).

2. **Log in**
   - Visit `http://127.0.0.1:8000/login`
   - Log in with the credentials you created.

3. **Search for POIs**
   - Go to `http://127.0.0.1:8000/search`
   - Select one or more **locations**.
   - Choose one or more **place types** from the dropdown (populated from `poi_searcher/data/sorted_place_types_list.txt`).
   - Choose the desired options (e.g. only place IDs vs. full extraction / live busyness) depending on the form controls.
   - Submit the form to create a **job**.

4. **Monitor jobs & download results**
   - Visit `http://127.0.0.1:8000/jobs` to see your jobs and their status.
   - Open a job detail page to download the output (CSV/ZIP) once it is completed.

---

### Output

The system generates data in two primary formats:

- **Busyness Data (Parquet/JSON)**: Saved in `live_busyness_extraction/output/` or `api_json_extraction/uploads/`.
    - Contains: `place_id`, `popular_times_histogram`, `live_busyness`, `timestamp`.
- **POI Results (CSV)**: Saved in `poi_searcher/output/`.
    - Contains: `place_id`, `name`, `type`, `location`, `address`.

---

### Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

--- 

### Contact
Email: shresthahewan12@gmail.com
