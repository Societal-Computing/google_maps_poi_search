# api_json_extraction/app/services/pipeline_bridge.py
import subprocess
import os
import re
import unicodedata
import pandas as pd
import logging
import shortuuid
import sys
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# Resolve project root relative to this file so the bridge works in any checkout path
_THIS_FILE = Path(__file__).resolve()
_APP_DIR = _THIS_FILE.parents[2]        # api_json_extraction
_PROJECT_ROOT = _APP_DIR.parent         # repository root


class PipelineBridge:
    """Bridge between the first and second pipelines"""
    
    def __init__(self, poi_searcher_path: str | None = None):
        """Initialize the bridge with path to the POI searcher"""
        if poi_searcher_path is None:
            poi_searcher_path = str(_PROJECT_ROOT / "poi_searcher")
        self.poi_searcher_path = poi_searcher_path
        
    def get_place_types(self):
        """Get list of available place types"""
        try:
            place_types_file = os.path.join(self.poi_searcher_path, "data", "sorted_place_types_list.txt")
            if os.path.exists(place_types_file):
                with open(place_types_file, "r") as f:
                    return [line.strip() for line in f if line.strip()]
            return []
        except Exception as e:
            logger.error(f"Error reading place types: {e}")
            return []
        
    def run_poi_search(self, location, place_types):
        """Run the first pipeline to get place IDs for multiple place types"""
        try:
            # Handle both single string and list of place types
            if isinstance(place_types, str):
                place_types = [place_types]
            
            # Convert list to comma-separated string for the poi_searcher
            place_types_str = ",".join(place_types)
            
            logger.info(f"Starting POI search for {location} - {place_types}")
            
            # Use a location-scoped output CSV to avoid overwrites across multiple locations
            # Slugify to strip special characters (e.g., umlauts)
            norm = unicodedata.normalize('NFKD', location)
            ascii_loc = norm.encode('ascii', 'ignore').decode('ascii')
            safe_loc = re.sub(r"\W+", "_", ascii_loc).strip('_')
            scoped_output = os.path.join(self.poi_searcher_path, "output", f"poi_results_{safe_loc}.csv")
            try:
                os.makedirs(os.path.dirname(scoped_output), exist_ok=True)
            except Exception:
                pass

            # Start fresh for this location run to avoid accumulating duplicates from previous runs
            try:
                if os.path.exists(scoped_output):
                    os.remove(scoped_output)
            except Exception:
                pass

            # Prefer running via `uv run` to ensure correct environment, fall back to current interpreter
            if shutil.which("uv"):
                command = [
                    "uv", "run",
                    "-m", "src.main",
                    "--location", location,
                    "--place_types", place_types_str,
                    "--skip-enrichment"
                ]
            else:
                command = [
                    sys.executable,
                    "-m", "src.main",
                    "--location", location,
                    "--place_types", place_types_str,
                    "--skip-enrichment"
                ]
            
            # Execute the command in the poi_searcher directory
            env = os.environ.copy()
            # Ensure a full stdlib-enabled Python is used irrespective of PATH
            env.pop("PYTHONHOME", None)
            # Disable JSON-type filtering for simple searches so rows without json details are kept
            env["POI_FILTER_BY_JSON_TYPE"] = "0"
            env["POI_OUTPUT_CSV_PATH"] = scoped_output
            # Inherit stdout/stderr so POI logs (phase counts, kept, etc.) are visible in API logs
            process = subprocess.run(
                command,
                cwd=self.poi_searcher_path,
                env=env
            )
            
            if process.returncode != 0:
                logger.error("POI search failed (see subprocess stderr above)")
                return None
            
            # Use the scoped output CSV file, wait briefly if needed
            output_csv = scoped_output
            if not os.path.exists(output_csv):
                try:
                    import time as _time
                    for _ in range(60):  # wait up to ~60s
                        if os.path.exists(output_csv) and os.path.getsize(output_csv) > 0:
                            break
                        _time.sleep(1)
                except Exception:
                    pass
            if os.path.exists(output_csv):
                # Report summary stats (JSON enrichment is handled later in API phase)
                try:
                    _df = pd.read_csv(output_csv)
                    total_rows = len(_df)
                    uniq_place_ids = _df['place_id'].astype(str).nunique() if 'place_id' in _df.columns else total_rows
                    logger.info(
                        f"POI search completed: rows={total_rows}, unique_place_ids={uniq_place_ids}. File: {output_csv}"
                    )
                except Exception:
                    logger.info(f"POI search completed. Results at {output_csv}")
                return output_csv
            else:
                logger.error(f"Output CSV not found at {output_csv}")
                # Fallback: find the most recent poi_results_*.csv in the output directory
                try:
                    from glob import glob
                    import time
                    out_dir = os.path.join(self.poi_searcher_path, "output")
                    pattern_a = glob(os.path.join(out_dir, "poi_results_*.csv"))
                    pattern_b = glob(os.path.join(out_dir, "poi_results.csv"))
                    candidates = sorted(
                        list(set(pattern_a + pattern_b)),
                        key=lambda p: os.path.getmtime(p),
                        reverse=True
                    )
                    if candidates:
                        latest = candidates[0]
                        # Only accept very recent files (within last 5 minutes) to avoid stale mismatch
                        if time.time() - os.path.getmtime(latest) <= 300:
                            logger.warning(f"Using latest fallback results file: {latest}")
                            return latest
                except Exception as _e:
                    logger.debug(f"Fallback search for results failed: {_e}")
                return None
                
        except Exception as e:
            logger.error(f"Error running POI search: {str(e)}")
            return None
            
    def prepare_extraction_job(self, place_ids_file, job_id=None):
        """Convert POI search results to a format suitable for extraction API"""
        try:
            # Use the provided job_id or generate a new one
            if job_id is None:
                job_id = shortuuid.uuid()
                
            base_dir = os.path.dirname(os.path.dirname(__file__))  # api_json_extraction
            job_file = os.path.join(base_dir, "uploads", f"job_{job_id}.csv")
            os.makedirs(os.path.dirname(job_file), exist_ok=True)
            
            # Read the CSV from the first pipeline
            df = pd.read_csv(place_ids_file)
            
            # Create a new DataFrame with just the place_id column
            if 'id' in df.columns:
                new_df = pd.DataFrame({'place_id': df['id']})
            elif 'place_id' in df.columns:
                new_df = pd.DataFrame({'place_id': df['place_id']})
            else:
                # If structure is different, use first column
                new_df = pd.DataFrame({'place_id': df.iloc[:, 0]})
            
            # Deduplicate by place_id to ensure uniqueness
            new_df = new_df.drop_duplicates(subset=["place_id"]).reset_index(drop=True)

            # Save the CSV for the second pipeline
            new_df.to_csv(job_file, index=False)
            
            logger.info(f"Created job file with {len(new_df)} place IDs: {job_file}")
            return job_file, job_id
            
        except Exception as e:
            logger.error(f"Error preparing extraction job: {str(e)}")
            return None, None
