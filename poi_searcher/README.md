<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://www.i2sc.net">
    <img src="figures/logo.png" ="Logo" width="800" height="110">
  </a>

  <h3 align="center">Google Places POI Searcher</h3>

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

This project is designed to search for Points of Interest (POIs) using the Google Places API. Currently, it uses Google Places TextSearch(New) API. However, it can be changed based on the requirement. Please refer to [`Prerequisities`](#prerequisites) to see what parameters can be changed. The search approach maintains a `task_queue` which contains (`poi_type,bounding_box`) as a pair. No API keys are assigned to any `bounding_box` or `poi_type`. The API keys are assigned in a round-robin fashion depending on the `task_queue`. The API keys work asynchronously as the tasks in `task_queue` are processed concurrently based on the availability of API keys. The results are saved in a CSV file and a JSON file is saved which keeps track of API requests count.

---

### Project Structure
```
├── data
│   ├── api_keys.txt
|   ├── config.yaml
|   ├── dudweiler.geojson 
|   └── poi_types_list.txt
├── figures
├── notebook
├── src
|   ├── api_key_manager.py
|   ├── bbox_utils.py
|   ├── data_manager.py
|   ├── main.py
|   └── poi_searcher.py
├── README.md
└── requirements.txt
```
---

### Prerequisites

Required parameters for `config.yaml`:
```
paths:
  api_keys: "data/api_keys.txt"
  poi_types: "data/poi_types_list.txt"
  output_csv: "output/poi_results.csv"
  city_geojson: "data/dudweiler.geojson"
  queue_log: "output/queue_tasks.txt"
  api_requests_json: "output/api_requests_count.json"

api:
  base_url: "https://places.googleapis.com/v1/places:searchText"
  field_mask: "places.id"
  max_retries: 5
  base_backoff: 2
  cooldown_time: 60

processing:
  threshold: 10
  coord_precision: 5
  workers_per_key: 1
```
---

### Installation

Clone the repository:
```bash
git clone https://github.com/Societal-Computing/google_maps_poi_search.git
```

Install the required dependencies:
```bash
pip install -r requirements.txt
```

---

### Running the Code:
To run the script, use the following command:
```bash
python -m src.main
```
OR
```bash
./run_script.sh
```
---

### Output
- POI Place IDs are saved to `output/poi_results.csv`
- API Usage Data is saved to `output/api_requests_count.json`

Each row in the output CSV contains the following columns:

```
place_id,type,<OSM geometry details columns>
```

Where <OSM geometry details columns> are columns such as `boundary`, `admin_level`, `name`, `display_name`, etc., extracted from OpenStreetMap for the searched location. These columns provide additional context about the location geometry (e.g., administrative level, type, and more).

---

### Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

--- 

### Contact
Email: shresthahewan12@gmail.com
