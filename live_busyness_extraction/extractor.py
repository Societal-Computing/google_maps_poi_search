import os
import json
from datetime import datetime
import logging
from typing import List, Tuple, Dict, Any, Set, Optional
import requests
import time
import random
from collections import defaultdict
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from zoneinfo import ZoneInfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# Force log timestamps to UTC for a common baseline across jobs
try:
    import time as _time
    logging.Formatter.converter = _time.gmtime
except Exception:
    pass
logger = logging.getLogger(__name__)

class LiveBusynessExtractor:
    def __init__(self, proxy_username: str, proxy_password: str):
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.proxies = []
        self.proxy_stats = defaultdict(lambda: {
            'success_count': 0,
            'failure_count': 0,
            'last_used': 0,
            'last_success': 0,
            'avg_response_time': 0,
            'is_active': True,
            'consecutive_failures': 0,
            'total_requests': 0,
            'error_types': defaultdict(int),
            'place_id_successes': set(),  # Track which place IDs succeeded with this proxy
            'place_id_failures': set()    # Track which place IDs failed with this proxy
        })
        self.current_proxy_index = 0
        self.max_retries = 3
        self.retry_delay = 2
        self.proxy_cooldown = 5  # seconds between requests for same proxy
        self.max_consecutive_failures = 3
        self.session = self._create_session()
        self.proxy_rotation_strategy = 'performance'  # Options: 'round_robin', 'performance'

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()
        # Disable urllib3 automatic retries; we handle cooldowns ourselves
        retry_strategy = Retry(
            total=0,
            connect=0,
            read=0,
            redirect=0,
            backoff_factor=0,
            status_forcelist=[],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        # Set CONSENT cookie once for the lifetime of this session. This reduces the chance
        # of hitting the GDPR/consent wall that can lead to "censored" JSON responses.
        try:
            session.cookies.set('CONSENT', 'YES+', domain='.google.com')
            session.cookies.set('CONSENT', 'YES+', domain='.google.com.maps')
        except Exception:
            pass
        return session

    def _parse_maps_json(self, raw_content: bytes, proxy: str, start_time: float, place_id: str) -> Optional[Any]:
        """Parse Google Maps JSON response body into Python object."""
        try:
            content = raw_content.decode('utf-8', errors='replace')
            # Remove )]}' prefix if present
            if content.startswith(")]}'"):
                content = content[4:]
            bracket_index = content.find('[')
            if bracket_index == -1:
                self._update_proxy_stats(proxy, False, time.time() - start_time, "no_bracket", place_id)
                return None
            json_content = content[bracket_index:]
            return json.loads(json_content)
        except json.JSONDecodeError:
            self._update_proxy_stats(proxy, False, time.time() - start_time, "json_decode_error", place_id)
            return None
        except Exception:
            self._update_proxy_stats(proxy, False, time.time() - start_time, "parse_error", place_id)
            return None

    def _has_popular_times_84(self, data: Any) -> bool:
        """Return True if data appears to include popular times at [6][84]."""
        try:
            return (
                isinstance(data, list)
                and len(data) > 6
                and isinstance(data[6], list)
                and len(data[6]) > 84
                and data[6][84] is not None
            )
        except Exception:
            return False

    def _get_headers(self, for_json: bool = False, referer: str = None) -> Dict[str, str]:
        """Get headers for requests. Use for_json=True for JSON API requests."""
        if for_json:
            # Headers for JSON API requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Origin': 'https://www.google.com',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
            }
            if referer:
                headers['Referer'] = referer
            return headers
        else:
            # Headers for HTML page requests
            return {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
            }

    def load_proxies(self, proxy_file: str) -> None:
        """Load proxies from a file and optionally perform health check."""
        try:
            with open(proxy_file, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(self.proxies)} proxies")
            
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            raise


    def _test_proxy(self, proxy: str) -> bool:
        """Test if a proxy is working."""
        try:
            formatted_proxy = self._format_proxy(proxy)
            proxies = {"http": formatted_proxy, "https": formatted_proxy}
            
            start_time = time.time()
            response = self.session.get(
                "https://www.google.com",
                proxies=proxies,
                timeout=10,
                headers=self._get_headers()
            )
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                self.proxy_stats[proxy]['avg_response_time'] = response_time
                return True
            return False
        except Exception as e:
            logger.debug(f"Proxy test failed for {proxy}: {e}")
            return False

    def _format_proxy(self, proxy: str) -> str:
        """Format proxy string with credentials."""
        return f"http://{self.proxy_username}:{self.proxy_password}@{proxy.split('://')[-1]}"

    def get_next_proxy(self, place_id: str = None) -> str:
        """Get the next working proxy using a smart selection strategy."""
        if not self.proxies:
            raise ValueError("No proxies available")
        
        current_time = time.time()
        
        # Filter available proxies
        available_proxies = [
            p for p in self.proxies
            if self.proxy_stats[p]['is_active'] and 
            (current_time - self.proxy_stats[p]['last_used']) >= self.proxy_cooldown
        ]
        
        if not available_proxies:
            logger.warning("No proxies available, waiting for cooldown...")
            time.sleep(self.proxy_cooldown)
            self._reset_proxy_stats()
            available_proxies = self.proxies
        
        # If we have a place_id, try to use a proxy that succeeded with it before
        if place_id:
            successful_proxies = [
                p for p in available_proxies
                if place_id in self.proxy_stats[p]['place_id_successes']
            ]
            if successful_proxies:
                available_proxies = successful_proxies
        
        # Select proxy based on strategy
        if self.proxy_rotation_strategy == 'round_robin':
            selected_proxy = available_proxies[self.current_proxy_index % len(available_proxies)]
            self.current_proxy_index += 1
        else:  # performance-based selection
            selected_proxy = min(
                available_proxies,
                key=lambda p: (
                    self.proxy_stats[p]['consecutive_failures'],
                    self.proxy_stats[p]['failure_count'] / (self.proxy_stats[p]['success_count'] + 1),
                    self.proxy_stats[p]['avg_response_time']
                )
            )
        
        self.proxy_stats[selected_proxy]['last_used'] = current_time
        return selected_proxy

    def _reset_proxy_stats(self) -> None:
        """Reset proxy statistics periodically."""
        for proxy in self.proxies:
            self.proxy_stats[proxy]['consecutive_failures'] = 0
            self.proxy_stats[proxy]['is_active'] = True

    def _update_proxy_stats(self, proxy: str, success: bool, response_time: float, error_type: str = None, place_id: str = None) -> None:
        """Update proxy statistics after a request."""
        stats = self.proxy_stats[proxy]
        if success:
            stats['success_count'] += 1
            stats['consecutive_failures'] = 0
            stats['last_success'] = time.time()
            stats['avg_response_time'] = (
                (stats['avg_response_time'] * (stats['success_count'] - 1) + response_time) /
                stats['success_count']
            )
            if place_id:
                stats['place_id_successes'].add(place_id)
                stats['place_id_failures'].discard(place_id)
        else:
            stats['failure_count'] += 1
            stats['consecutive_failures'] += 1
            if error_type:
                stats['error_types'][error_type] += 1
            if place_id:
                stats['place_id_failures'].add(place_id)
            
            if stats['consecutive_failures'] >= self.max_consecutive_failures:
                stats['is_active'] = False
                logger.warning(f"Proxy {proxy} deactivated due to {self.max_consecutive_failures} consecutive failures")

    def get_json_url(self, place_id: str, proxy: str) -> str:
        """Get the JSON URL for a place ID. Also establishes session cookies by visiting the place page."""
        start_time = time.time()
        try:
            google_search_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
            formatted_proxy = self._format_proxy(proxy)
            proxies = {"http": formatted_proxy, "https": formatted_proxy}
            
            # Visit the place page to establish session and get cookies
            # This helps bypass Google's anti-scraping measures
            response = self.session.get(
                google_search_url,
                proxies=proxies,
                timeout=15,
                headers=self._get_headers(for_json=False)
            )
            
            if response.status_code != 200:
                self._update_proxy_stats(proxy, False, time.time() - start_time, f"status_{response.status_code}", place_id)
                logger.error(f"Failed to fetch URL for place_id {place_id}. Status code: {response.status_code}")
                return None

            # Small delay to simulate human behavior
            time.sleep(random.uniform(0.01, 0.2))

            content = response.content.decode('utf-8')
            id_pair = self._extract_id_pair(content)
            if not id_pair:
                self._update_proxy_stats(proxy, False, time.time() - start_time, "no_id_pair", place_id)
                logger.error(f"No id_pair found for place_id {place_id}")
                return None

            pb = self._get_pb(id_pair)
            json_url = f"https://www.google.com/maps/preview/place?authuser=0&hl=en&gl=de&pb={pb}&pf=t"
            self._update_proxy_stats(proxy, True, time.time() - start_time, None, place_id)
            return json_url

        except Exception as e:
            self._update_proxy_stats(proxy, False, time.time() - start_time, str(type(e).__name__), place_id)
            logger.error(f"Error getting JSON URL for place_id {place_id}: {e}")
            return None

    def get_json_data(self, json_url: str, proxy: str, place_id: str) -> Dict:
        """
        Get JSON data from the URL. 
        Performance-optimized flow:
        - Fast-path: try fetching the JSON directly using the existing session/cookies.
          If it's "censored" (popular times missing at [6][84]), then fall back to the
          2-step browser-like flow (visit place page -> retry JSON).
        """
        start_time = time.time()
        try:
            formatted_proxy = self._format_proxy(proxy)
            proxies = {"http": formatted_proxy, "https": formatted_proxy}

            place_page_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

            # Common JSON headers
            json_headers_base = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
            }

            # FAST-PATH: try JSON directly first (no place-page visit)
            direct_headers = dict(json_headers_base)
            direct_headers['Referer'] = 'https://www.google.com/maps'
            direct_resp = self.session.get(
                json_url,
                proxies=proxies,
                timeout=15,
                headers=direct_headers
            )
            if direct_resp.status_code == 200:
                direct_data = self._parse_maps_json(direct_resp.content, proxy, start_time, place_id)
                if direct_data is not None and self._has_popular_times_84(direct_data):
                    self._update_proxy_stats(proxy, True, time.time() - start_time, None, place_id)
                    return direct_data
            else:
                # If direct request fails, proceed with fallback flow
                logger.debug(f"Direct JSON fetch failed (status {direct_resp.status_code}) for place_id {place_id}; falling back to preflight.")
            
            # FALLBACK: 2-step browser-like flow (place page -> JSON)
            # Headers for place page visit (match fetch_json_request.py)
            place_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
            }
            
            # Visit place page to get initial cookies
            place_response = self.session.get(
                place_page_url,
                proxies=proxies,
                timeout=15,
                headers=place_headers
            )
            
            if place_response.status_code != 200:
                self._update_proxy_stats(proxy, False, time.time() - start_time, f"place_page_status_{place_response.status_code}", place_id)
                logger.error(f"Failed to visit place page for place_id {place_id}. Status code: {place_response.status_code}")
                # Return direct_data (even if censored) if we at least parsed something
                return direct_data if 'direct_data' in locals() else None
            
            # Set CONSENT cookie explicitly to bypass GDPR/privacy consent wall
            # This is critical for getting full data instead of censored responses
            self.session.cookies.set('CONSENT', 'YES+', domain='.google.com')
            self.session.cookies.set('CONSENT', 'YES+', domain='.google.com.maps')
            
            # Small delay; we already did one direct attempt above, so keep this very short
            time.sleep(random.uniform(0.01, 0.2))

            # STEP 2: JSON request with Referer = place page we just visited
            json_headers = dict(json_headers_base)
            json_headers['Referer'] = place_page_url
            
            # Make the JSON request with cookies from the session
            # Increased timeout to 15s as the full response (7000 lines) takes longer to download
            response = self.session.get(
                json_url,
                proxies=proxies,
                timeout=15,
                headers=json_headers
            )
            
            if response.status_code != 200:
                self._update_proxy_stats(proxy, False, time.time() - start_time, f"status_{response.status_code}", place_id)
                logger.error(f"Failed to fetch JSON data. Status code: {response.status_code}")
                return direct_data if 'direct_data' in locals() else None

            data = self._parse_maps_json(response.content, proxy, start_time, place_id)
            if data is not None:
                self._update_proxy_stats(proxy, True, time.time() - start_time, None, place_id)
                return data

            # Final fallback: return direct data if we had it parsed
            return direct_data if 'direct_data' in locals() else None

        except Exception as e:
            self._update_proxy_stats(proxy, False, time.time() - start_time, str(type(e).__name__), place_id)
            logger.error(f"Error getting JSON data: {e}")
            return None

    def _extract_id_pair(self, content: str) -> str:
        """Extract the ID pair from the content."""
        import re
        pattern = r'(?!0x0:0x)0x[0-9a-f]+:0x[0-9a-f]+'
        found_text = re.findall(pattern, content)
        return found_text[0] if found_text else None

    def _get_pb(self, ids: str) -> str:
        """Generate the pb parameter for the JSON URL."""
        pb_template = '!1m17!1s{0}!3m12!1m3!1d442.3895626117454!2d51.52449680499801!3d25.37702882842613!2m3!1f0!2f0!3f0!3m2!1i1280!2i377!4f13.1!4m2!3d25.37705656392284!4d51.52511112391948!12m4!2m3!1i360!2i120!4i8!13m57!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m42!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e3!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e3!2b1!3e2!1m3!1e9!2b1!3e2!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!2b1!4b1!9b0!14m5!1sLMySXoeYBI6Ck74P-9a24AU!4m1!2i5210!7e81!12e3!15m49!1m13!4e2!13m6!2b1!3b1!4b1!6i1!8b1!9b1!18m4!3b1!4b1!5b1!6b1!2b1!5m5!2b1!3b1!5b1!6b1!7b1!10m1!8e3!14m1!3b1!17b1!20m2!1e3!1e6!24b1!25b1!26b1!30m1!2b1!36b1!43b1!52b1!54m1!1b1!55b1!56m2!1b1!3b1!65m5!3m4!1m3!1m2!1i224!2i298!21m28!1m6!1m2!1i0!2i0!2m2!1i458!2i377!1m6!1m2!1i1230!2i0!2m2!1i1280!2i377!1m6!1m2!1i0!2i0!2m2!1i1280!2i20!1m6!1m2!1i0!2i357!2m2!1i1280!2i377!22m1!1e81!29m0!30m1!3b1'
        return pb_template.format(ids)

    def extract_busyness_data(self, json_data: Dict) -> Dict[str, Any]:
        """Extract busyness data from JSON content."""
        try:
            if not json_data:
                return {
                    "current_hour": None,
                    "original_busyness": None,
                    "current_busyness": None,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

            # Extract timing information (index 6, 84)
            timing = json_data[6][84] if len(json_data) > 6 and len(json_data[6]) > 84 else None
            if not timing:
                return {
                    "current_hour": None,
                    "original_busyness": None,
                    "current_busyness": None,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

            current_day = timing[1]
            if current_day == 0:
                current_day = 7
            current_hour = timing[4]
            busyness_baseline = timing[0]
            original_busyness = None

            # Debug: log the structure
            logger.debug(f"busyness_baseline: {busyness_baseline}")
            logger.debug(f"current_day: {current_day}, current_hour: {current_hour}")

            # Dynamically find the correct day index for hour_list
            hour_list = None
            found_index = None
            for idx, day_data in enumerate(busyness_baseline):
                try:
                    candidate_hour_list = day_data[1]
                    if isinstance(candidate_hour_list, list):
                        # If the current hour is present in this day's hour list, use it
                        if any(hour_data[0] == current_hour for hour_data in candidate_hour_list):
                            hour_list = candidate_hour_list
                            found_index = idx
                            break
                except Exception:
                    continue

            if hour_list is not None:
                # logger.info(f"Found hour_list at index {found_index} for current_hour={current_hour}")
                pass
            else:
                logger.error("Could not find a valid hour_list for current_hour in any day index")

            if isinstance(hour_list, list):
                for hour_data in hour_list:
                    if hour_data[0] == current_hour:
                        original_busyness = hour_data[1]
                        break
                if original_busyness is None:
                    logger.warning(f"No matching hour found for current_hour={current_hour} in hour_list: {hour_list}")
            else:
                logger.warning(f"hour_list is not a list for current_day={current_day}: {hour_list}")

            # Get current busyness
            if len(timing) == 8:
                current_busyness = timing[7][1]
            else:
                current_busyness = None

            if not original_busyness:
                current_busyness = None

            return {
                "current_hour": current_hour,
                "original_busyness": original_busyness,
                "current_busyness": current_busyness,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            logger.error(f"Error extracting busyness data: {e}")
            return {
                "current_hour": None,
                "original_busyness": None,
                "current_busyness": None,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

    def extract_place_id(self, json_data: Dict) -> str:
        """Extract place ID from JSON content."""
        try:
            if not json_data or len(json_data) <= 6 or len(json_data[6]) <= 18:
                return None
            return json_data[6][78]
        except Exception as e:
            logger.error(f"Error extracting place ID: {e}")
            return None

    def extract_place_name(self, json_data: Dict) -> str:
        """Extract place name from JSON content."""
        try:
            if not json_data or len(json_data) <= 6 or len(json_data[6]) <= 18:
                return None
            return json_data[6][11]
        except Exception as e:
            logger.error(f"Error extracting place name: {e}")
            return None

    def extract_place_types(self, json_data: Dict) -> str:
        """Extract place type from JSON content."""
        try:
            if not json_data or len(json_data) <= 6 or len(json_data[6]) <= 18:
                return None
            return json_data[6][13]
        except Exception as e:
            logger.error(f"Error extracting place type: {e}")
            return None
        
    def extract_place_address(self, json_data: Dict) -> str:
        """Extract place address from JSON content."""
        try:
            if not json_data or len(json_data) <= 6 or len(json_data[6]) <= 18:
                return None
            return json_data[6][39]
        except Exception as e:
            logger.error(f"Error extracting place address: {e}")
            return None

    def extract_coordinates(self, json_data: Dict) -> dict:
        """Extract latitude and longitude from JSON content and return as {'lat': float|None, 'lng': float|None}."""
        try:
            if (
                not json_data
                or len(json_data) <= 6
                or not isinstance(json_data[6], list)
                or len(json_data[6]) <= 9
                or not isinstance(json_data[6][9], list)
                or len(json_data[6][9]) <= 3
            ):
                return {'lat': None, 'lng': None}
            
            # Primary expected location (as per new indexing)
            try:
                location_data = json_data[6][9]
                lat = location_data[2]
                lng = location_data[3]
                if lat is not None and lng is not None:
                    return {'lat': float(lat), 'lng': float(lng)}
            except Exception:
                pass

            # Fallback: heuristic search for plausible (lat, lng) pair
            # We search nested lists/dicts up to a reasonable depth
            def is_number(x: Any) -> bool:
                try:
                    float(x)
                    return True
                except Exception:
                    return False

            def looks_like_lat_lng(a: Any, b: Any) -> dict:
                if not (is_number(a) and is_number(b)):
                    return None
                fa, fb = float(a), float(b)
                # Case 1: [lng, lat]
                if -180.0 <= fa <= 180.0 and -90.0 <= fb <= 90.0:
                    return {'lat': fb, 'lng': fa}
                # Case 2: [lat, lng]
                if -90.0 <= fa <= 90.0 and -180.0 <= fb <= 180.0:
                    return {'lat': fa, 'lng': fb}
                return None

            def dfs(node: Any, depth: int = 0) -> dict:
                if depth > 6 or node is None:
                    return None
                # Look for short sequences with two numbers
                if isinstance(node, (list, tuple)):
                    n = len(node)
                    # sliding window of size 2
                    for i in range(max(0, n - 1)):
                        candidate = looks_like_lat_lng(node[i], node[i + 1])
                        if candidate:
                            return candidate
                    # Recurse into children
                    for child in node:
                        found = dfs(child, depth + 1)
                        if found:
                            return found
                elif isinstance(node, dict):
                    # Try common key names first
                    keys = {k.lower(): k for k in node.keys() if isinstance(k, str)}
                    if 'lat' in keys and 'lng' in keys and is_number(node[keys['lat']]) and is_number(node[keys['lng']]):
                        return {'lat': float(node[keys['lat']]), 'lng': float(node[keys['lng']])}
                    for v in node.values():
                        found = dfs(v, depth + 1)
                        if found:
                            return found
                return None

            found_coords = dfs(json_data, 0)
            if found_coords:
                return found_coords
            return {'lat': None, 'lng': None}
        except Exception as e:
            logger.error(f"Error extracting coordinates: {e}")
            return {'lat': None, 'lng': None}

    def extract_timezone(self, json_data: Dict) -> Optional[str]:
        """Extract timezone information from the JSON content if available."""
        try:
            if (
                isinstance(json_data, list)
                and len(json_data) > 31
                and isinstance(json_data[31], list)
                and len(json_data[31]) > 1
                and isinstance(json_data[31][1], list)
                and len(json_data[31][1]) > 0
                and isinstance(json_data[31][1][0], list)
                and len(json_data[31][1][0]) > 0
            ):
                tz_value = json_data[31][1][0][0]
                if isinstance(tz_value, str) and tz_value:
                    return tz_value
        except Exception as e:
            logger.debug(f"Failed to extract timezone: {e}")
        return None

    def extract_popular_times_live_text(self, json_data: Dict) -> str:
        """Extract 'popularTimesLiveText' from JSON content."""
        try:
            if (
                not json_data
                or len(json_data) <= 6
                or len(json_data[6]) <= 84
                or not isinstance(json_data[6][84], list)
                or len(json_data[6][84]) <= 6
            ):
                return None
            return json_data[6][84][6]
        except Exception as e:
            logger.error(f"Error extracting popularTimesLiveText: {e}")
            return None

    def extract_opening_hours(self, json_data: Dict) -> dict:
        """
        Extract 'openingHours' from JSON content.
        Returns a dict mapping day names to a list of opening hour strings,
        always ordered from Monday to Sunday, even if the first row is the scrape day.
        
        Tries two structures:
        1. json_data[6][34][1][i] where [i][0] = day name, [i][1] = hours
        2. json_data[6][203][0][i] where [i][0] = day name + "current day", [i][3][0][0] = hours string
        """
        try:
            # Helper for normalizing text
            def _normalize_text(text: str) -> str:
                if not isinstance(text, str):
                    return text
                replacements = {
                    '\u202f': ' ',  # narrow no-break space → space
                    '\u00a0': ' ',  # non-breaking space → space
                    '\u2013': ' - ',  # en dash → hyphen surrounded by spaces
                    '\u2014': ' - ',  # em dash → hyphen surrounded by spaces
                }
                for src, dst in replacements.items():
                    text = text.replace(src, dst)
                while '  ' in text:
                    text = text.replace('  ', ' ')
                return text.strip()

            # Standard order of days
            week_days = [
                "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
            ]

            # Helper to match day name to standard day
            def match_day_name(day_name: str) -> str:
                if not isinstance(day_name, str):
                    return None
                day_lower = day_name.strip().lower()
                for wd in week_days:
                    if day_lower.startswith(wd.lower()[:3]):
                        return wd
                return None

            # Helper to extract hours from various formats
            def extract_hours_value(hours_node):
                if isinstance(hours_node, str):
                    return [_normalize_text(hours_node)]
                elif isinstance(hours_node, list):
                    result = []
                    for item in hours_node:
                        if isinstance(item, str):
                            result.append(_normalize_text(item))
                        elif isinstance(item, list):
                            # Recursively search for strings
                            for subitem in item:
                                if isinstance(subitem, str):
                                    result.append(_normalize_text(subitem))
                                    break
                    return result if result else None
                return None

            day_to_hours = {day: None for day in week_days}
            opening_hours = None

            # Method 1: Try json_data[6][34][1][i] structure
            # Structure: [i][0] = day name, [i][1] = hours (list or string)
            try:
                if (
                    json_data
                    and isinstance(json_data, list)
                    and len(json_data) > 6
                    and isinstance(json_data[6], list)
                    and len(json_data[6]) > 34
                    and isinstance(json_data[6][34], list)
                    and len(json_data[6][34]) > 1
                    and isinstance(json_data[6][34][1], list)
                ):
                    opening_hours_raw = json_data[6][34][1]
                    for day_entry in opening_hours_raw:
                        if not isinstance(day_entry, list) or len(day_entry) < 2:
                            continue
                        day_name = day_entry[0]
                        hours = day_entry[1]
                        matched_day = match_day_name(day_name)
                        if matched_day:
                            hours_list = extract_hours_value(hours)
                            if hours_list:
                                day_to_hours[matched_day] = hours_list
                    opening_hours = {day: hours for day, hours in day_to_hours.items() if hours is not None}
            except Exception as e:
                logger.debug(f"Error in method 1 (6][34][1]): {e}")

            # Method 2: Try json_data[6][203][0][i] structure
            # Structure (as observed in current responses):
            #   - json_data[6][203][0][i][0]        → day label (e.g. "Tuesday" or "Tuesday current day")
            #   - json_data[6][203][0][i][3][0][0] → opening hours string (e.g. "8 am - 8 pm")
            if not opening_hours:
                try:
                    if (
                        json_data
                        and isinstance(json_data, list)
                        and len(json_data) > 6
                        and isinstance(json_data[6], list)
                        and len(json_data[6]) > 203
                        and isinstance(json_data[6][203], list)
                        and len(json_data[6][203]) > 0
                        and isinstance(json_data[6][203][0], list)
                    ):
                        alt_list = json_data[6][203][0]
                        day_to_hours = {day: None for day in week_days}
                        for entry in alt_list:
                            if not isinstance(entry, list) or len(entry) <= 3:
                                continue

                            # Extract day name from entry[0] (may contain "current day" suffix)
                            day_label_raw = entry[0] if isinstance(entry[0], str) else None
                            if not day_label_raw:
                                continue

                            # Clean day label (remove "current day" or similar suffixes)
                            day_label = day_label_raw.strip()
                            matched_day = match_day_name(day_label)
                            if not matched_day:
                                # Try removing common suffixes
                                for suffix in [" current day", " (current day)", " today"]:
                                    if day_label.lower().endswith(suffix.lower()):
                                        day_label = day_label[:-len(suffix)].strip()
                                        matched_day = match_day_name(day_label)
                                        break

                            # Extract hours strictly from entry[3][0][0] as observed
                            hours_text = None
                            if (
                                isinstance(entry[3], list)
                                and len(entry[3]) > 0
                                and isinstance(entry[3][0], list)
                                and len(entry[3][0]) > 0
                                and isinstance(entry[3][0][0], str)
                            ):
                                hours_text = entry[3][0][0]

                            if matched_day and hours_text:
                                day_to_hours[matched_day] = [_normalize_text(hours_text)]
                        
                        opening_hours = {day: hours for day, hours in day_to_hours.items() if hours is not None}
                except Exception as e:
                    logger.debug(f"Error in method 2 ([6][203][0]): {e}")

            return opening_hours if opening_hours else None
        except Exception as e:
            logger.error(f"Error extracting openingHours: {e}")
            return None

    def extract_popular_times_histogram(self, json_data: Dict) -> dict:
        """
        Extract 'popularTimesHistogram' from JSON content.
        Returns a dict mapping day label ('Monday', ...) to a list of dicts:
        [
            {"hour": int, "occupancyPercent": int},
            ...
        ]
        The first row in the raw data may correspond to any day of the week (e.g., if scraped on Wednesday, first row is Wednesday).
        The final response is always sorted from Monday to Sunday.
        """
        try:
            if not json_data or len(json_data) <= 6 or len(json_data[6]) <= 84:
                return None

            node = json_data[6][84]
            if not isinstance(node, list) or not node:
                return None

            # New, explicit structure (as per latest observations):
            # json_data[6][84][0][i][0] -> day code (int), where
            #   7 = Sunday, 1 = Monday, ..., 6 = Saturday
            # json_data[6][84][0][i][1] -> list of [hour, occupancyPercent, ...]
            histogram_raw = node[0]
            if not isinstance(histogram_raw, list) or len(histogram_raw) < 5:
                logger.debug("popularTimesHistogram: [6][84][0] is not a valid days array")
                return None

            # Build hours list for each day entry
            def build_hours_list(day_entry):
                hours_list = []
                if not (
                    isinstance(day_entry, list)
                    and len(day_entry) > 1
                    and isinstance(day_entry[1], list)
                ):
                    return hours_list
                for hour_entry in day_entry[1]:
                    if (
                        isinstance(hour_entry, list)
                        and len(hour_entry) >= 2
                        and isinstance(hour_entry[0], int)
                        and isinstance(hour_entry[1], int)
                    ):
                        hours_list.append(
                            {
                                "hour": hour_entry[0],
                                "occupancyPercent": hour_entry[1],
                            }
                        )
                return hours_list

            # Map numeric day codes to canonical day names
            code_to_label = {
                1: "Monday",
                2: "Tuesday",
                3: "Wednesday",
                4: "Thursday",
                5: "Friday",
                6: "Saturday",
                7: "Sunday",
            }

            # First collect per-day data into a temporary map
            labeled_histogram = {}
            for day_entry in histogram_raw:
                if not (isinstance(day_entry, list) and len(day_entry) > 1):
                    continue

                day_code = day_entry[0]
                if not isinstance(day_code, int):
                    continue

                label = code_to_label.get(day_code)
                if not label:
                    continue

                hours_list = build_hours_list(day_entry)
                if hours_list:
                    labeled_histogram[label] = hours_list

            # Re-order into canonical Monday..Sunday sequence for output
            ordered_histogram = {}
            canonical_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            for day_name in canonical_order:
                if day_name in labeled_histogram:
                    ordered_histogram[day_name] = labeled_histogram[day_name]

            return ordered_histogram if ordered_histogram else None
        except Exception as e:
            logger.error(f"Error extracting popularTimesHistogram: {e}")
            return None
  
    def extract_reviews_count(self, json_data: Dict) -> int:
        """Extract reviews count from JSON content."""
        try:
            if not json_data or len(json_data) <= 6 or len(json_data[6]) <= 18:
                return None
            return json_data[6][4][8]
        except Exception as e:
            # logger.error(f"Error extracting reviews count: {e}")
            return None
    
    def extract_reviews_distribution(self, json_data: Dict) -> dict:
        """
        Extract reviews distribution from JSON content.
        Returns a dict with keys: oneStar, twoStar, threeStar, fourStar, fiveStar.
        """
        try:
            # Fast path: expected location
            node6 = json_data[6] if isinstance(json_data, list) and len(json_data) > 6 else None
            if isinstance(node6, list) and len(node6) > 175:
                node175 = node6[175]
                if isinstance(node175, list) and len(node175) > 3:
                    dist_node = node175[3]
                    # Case A: dict with keys 0..4 (or '0'..'4')
                    if isinstance(dist_node, dict):
                        def get_key(d, k):
                            return d.get(k, d.get(str(k), 0))
                        vals = [get_key(dist_node, i) for i in range(5)]
                        if all(isinstance(v, int) and v >= 0 for v in vals):
                            return {
                                "oneStar": vals[0],
                                "twoStar": vals[1],
                                "threeStar": vals[2],
                                "fourStar": vals[3],
                                "fiveStar": vals[4],
                            }
                    # Case B: list of 5 ints
                    if isinstance(dist_node, list) and len(dist_node) >= 5 and all(isinstance(x, int) for x in dist_node[:5]):
                        return {
                            "oneStar": dist_node[0],
                            "twoStar": dist_node[1],
                            "threeStar": dist_node[2],
                            "fourStar": dist_node[3],
                            "fiveStar": dist_node[4],
                        }

            # Fallback: heuristic search under json_data[6] for dict 0..4 or list of 5 ints
            target = node6
            def dfs(node, depth=0):
                if depth > 6:
                    return None
                if isinstance(node, dict):
                    # dict with keys 0..4 (int or str)
                    keys = set(node.keys())
                    has_all = all((k in keys or str(k) in keys) for k in range(5))
                    if has_all:
                        def get_key(d, k):
                            return d.get(k, d.get(str(k), 0))
                        vals = [get_key(node, i) for i in range(5)]
                        if all(isinstance(v, int) and v >= 0 for v in vals):
                            return vals
                    for v in node.values():
                        res = dfs(v, depth + 1)
                        if res is not None:
                            return res
                elif isinstance(node, list):
                    if len(node) >= 5 and all(isinstance(x, int) for x in node[:5]):
                        return node[:5]
                    for child in node:
                        res = dfs(child, depth + 1)
                        if res is not None:
                            return res
                return None

            if isinstance(target, (list, dict)):
                vals = dfs(target)
                if vals is not None:
                    return {
                        "oneStar": vals[0],
                        "twoStar": vals[1],
                        "threeStar": vals[2],
                        "fourStar": vals[3],
                        "fiveStar": vals[4],
                    }
            return None
        except Exception as e:
            logger.error(f"Error extracting reviews distribution: {e}")
            return None

    def process_place_id(self, place_id: str, place_type: str) -> Tuple[str, str, str, Any, Any, Dict, bool]:
        """Process a single place ID to get its busyness data.
        Returns: (place_id, place_name, place_type, place_address, location_list, busyness_data, success_flag)
        where location_list is [lat, lng].
        """
        proxy = self.get_next_proxy(place_id)
        try:
            json_url = self.get_json_url(place_id, proxy)
            if not json_url:
                logger.warning(f"Failed to get JSON URL for place_id {place_id}")
                return place_id, None, place_type, {
                    "current_hour": None,
                    "original_busyness": None,
                    "current_busyness": None,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, False

            json_data = self.get_json_data(json_url, proxy, place_id)
            if not json_data:
                logger.warning(f"Failed to get JSON data for place_id {place_id}")
                return place_id, None, place_type, {
                    "current_hour": None,
                    "original_busyness": None,
                    "current_busyness": None,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, False

            busyness_data = self.extract_busyness_data(json_data)
            place_name = self.extract_place_name(json_data)
            place_address = self.extract_place_address(json_data)
            location = self.extract_coordinates(json_data)  # {"location": [lat, lng]}
            reviews_count = self.extract_reviews_count(json_data)
            reviews_distribution = self.extract_reviews_distribution(json_data)
            timezone_name = self.extract_timezone(json_data)
            
            # # Add coordinates to busyness data
            # busyness_data["latitude"] = lat
            # busyness_data["longitude"] = lng
            
            # Check if we got valid data
            if busyness_data["current_busyness"] is not None:
                # Attach reviews info to result bundle
                busyness_data = dict(busyness_data)
                busyness_data["reviews_count"] = reviews_count
                busyness_data["reviews_distribution"] = reviews_distribution
                if timezone_name:
                    busyness_data["timezone"] = timezone_name
                if isinstance(location, dict):
                    loc_list = [location.get('lat'), location.get('lng')]
                elif isinstance(location, (list, tuple)):
                    loc_list = list(location)
                else:
                    loc_list = [None, None]
                return place_id, place_name, place_type, place_address, loc_list, busyness_data, True
            else:
                logger.warning(f"Got empty busyness data for place_id {place_id}")
                failure_payload = {
                    "current_hour": None,
                    "original_busyness": None,
                    "current_busyness": None,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                if timezone_name:
                    failure_payload["timezone"] = timezone_name
                return place_id, None, place_type, place_address, [None, None], failure_payload, False

        except Exception as e:
            logger.error(f"Error processing place_id {place_id}: {e}")
            failure_payload = {
                "current_hour": None,
                "original_busyness": None,
                "current_busyness": None,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            try:
                if timezone_name:
                    failure_payload["timezone"] = timezone_name
            except UnboundLocalError:
                pass
            return place_id, None, place_type, None, [None, None], failure_payload, False

    def save_results(self, results: List[Tuple[str, str, str, Dict]], output_file: str, city: str = None) -> None:
        """Save results to JSON files only in city-specific folders."""
        try:
            # Save JSON file only
            if city:
                self._save_json_results(results, city, output_file)
            else:
                logger.warning("No city provided, cannot save JSON results")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            raise

    def _save_json_results(self, results: List[Tuple], city: str, output_file: str) -> None:
        """Save results to a JSON file with the specified format in a city-specific folder."""
        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_file)
            
            # Create city-specific folder within the output directory
            city_folder = os.path.join(output_dir, city)
            os.makedirs(city_folder, exist_ok=True)
            
            # Create timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{city}_{timestamp}.json"
            filepath = os.path.join(city_folder, filename)
            
            # Convert results to the required JSON format
            json_data = []
            for item in results:
                # Support both legacy 5-tuple and new 6-tuple with location
                if len(item) >= 6:
                    place_id, place_name, place_type, place_address, location_list, result = item[:6]
                else:
                    # legacy: (place_id, place_name, place_type, place_address, result)
                    place_id, place_name, place_type, place_address, result = item
                    location_list = [None, None]
                # Timestamps
                from datetime import timezone as _tz
                utc_now = datetime.now(_tz.utc)
                # Compute best-effort local time using coords if available
                local_iso = None
                try:
                    tz_name = result.get('timezone')
                    if isinstance(tz_name, str) and tz_name:
                        local_dt = utc_now.astimezone(ZoneInfo(tz_name))
                        local_iso = local_dt.strftime('%Y-%m-%dT%H:%M:%S')
                    else:
                        local_iso = None
                except Exception:
                    local_iso = None
                json_entry = {
                    'placeId': place_id,
                    'title': place_name,
                    'address': place_address,
                    'location': location_list,
                    'reviewsCount': result.get('reviews_count'),
                    'reviewsDistribution': result.get('reviews_distribution'),
                    'currentHour': result.get('current_hour'),
                    'popularTimesHistogram': result.get('original_busyness'),
                    'popularTimesLivePercent': result.get('current_busyness'),
                    'timezone': result.get('timezone'),
                    'scrapedAtUTC': utc_now.strftime('%Y-%m-%dT%H:%M:%S'),
                    'scrapedAtLocal': local_iso
                }
                json_data.append(json_entry)
            
            # Save JSON data
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Results saved to JSON file: {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving JSON results: {e}")
            raise