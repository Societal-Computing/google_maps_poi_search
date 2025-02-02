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
- Google API keys (stored in `data/api_keys.txt`)
- GeoJSON file for the city you want to search (stored in `data/dudweiler.geojson`)
- Text files containing POI types (stored in `data/poi_types_list.txt`)

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
