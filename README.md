<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://www.i2sc.net">
    <img src="figures/logo.png" ="Logo" width="800" height="110">
  </a>

  <h3 align="center">Google Places TextSearch(New) API Scraper</h3>

</div>

This project is designed to search for Points of Interest (POIs) using the Google Places API. It uses a quadtree-based approach to divide a city's bounding box into smaller regions and performs concurrent API requests to retrieve POI data. The results are saved in a CSV file, and API usage is logged in a JSON file.


---

## **Table of Contents**
1. [Project Structure](#project-structure)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Running the Code](#running-the-code)
5. [Output](#output)
6. [Contributing](#contributing)
7. [Contact](#contact)

---

### Project Structure
```
├── data
│   ├── api_keys.txt
|   ├── config.yaml
|   ├── dudweiler.geojson 
|   └── api_keys.txt
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
---

### Output
- POI Place IDs are saved to `output/poi_results.csv`
- API Usage Data is saved to `output/api_requests_count.json`

---

### Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

--- 

### Contact
Email: shresthahewan12@gmail.com
