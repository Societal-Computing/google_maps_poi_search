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
1. [Features](#features)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
6. [Running the Code](#running-the-code)
7. [Output](#output)
8. [Contributing](#contributing)
9. [Contact](#contact)

---

### Features
- Asynchronous API requests for improved performance
- Quadtree search algorithm for efficient area coverage
- Dynamic API key rotation to manage usage limits
- Configurable concurrent request limits
- Detailed logging of the scraping process
- CSV output for easy data analysis

---

### Project Structure
```
├── data
│   ├── chunks_new
|   ├── input_geojson
|   |    └── state_capital_geojsons
|   └── api_keys.txt
├── figures
├── src
|   ├── api_manager.py
|   ├── config.py
|   ├── logger.py
|   └── places_scraper.py
├── main.py
├── README.md
└── requirements.txt
```
---

### Prerequisites
- Python 3.8 or higher
- Google Places API keys (stored in `data/api_keys.txt`)
- GeoJSON file for the city you want to search
- Text files containing POI types (stored in `data/chunks_new`)

---

### Installation

Clone the repository:
```bash
git clone https://github.com/Societal-Computing/google_maps_poi_search.git
```

Install the required dependencies:
```bash
pip3 install -r requirements.txt
```

---

### Running the Code:
To run the script, use the following command:
```bash
python3 main.py --base-dir "." --geojson-file "data/input_geojson/state_capital_geojsons/berlin.geojson" --threshold 10 --max-concurrent 250 --log-level INFO
```

---

Command-Line Arguments:
- `--base-dir`: Base directory for the project
- `--geojson-file`: Path to input GeoJSON file defining the search area
- `--threshold`: Threshold for Quadtree search
- `--max-concurrent`: Maximum concurrent API Requests
- `--log-level`: Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

---

### Output
- POI Data is saved to `output/results.csv`
- API Usage Data is saved to `output/api_usage.json`
- Logs are wrriten to `logs/scraper_logs.txt`

---

### Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

--- 

### Contact
Email: shresthahewan12@gmail.com
