from helper import *
from insert import *


def insert_data_from_json(json_data: json):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        insert_data(cur, json_data)
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


class DetailScraper:
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.session = requests.Session()
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                " AppleWebKit/537.36 (KHTML, like Gecko)"
                " Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
            "Referer": "https://www.sreality.cz/",
        }
        self.session.headers.update(headers)
        retries = Retry(total=5, backoff_factor=0.3,
                        status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def fetch_details_batch(self, links: List[str]) -> pd.DataFrame:
        """
        Accepts a list of API URLs, fetches them, and returns a DataFrame.
        """
        results = []

        def fetch_detail(url: str, web_url: str) -> Optional[Dict]:
            try:
                time.sleep(random.uniform(0.1, 0.4))
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                data = response.json()
                data["real_website_link"] = web_url
                insert_data_from_json(data)
                return data
            except Exception as e:
                logger.error(f"Error fetching detail {url}: {e}")
                return None

        logger.info(f"Starting detail scrape for {len(links)} properties...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(fetch_detail, url[0], url[1]): url for url in links}

            for future in tqdm(as_completed(future_to_url), total=len(links), desc="Scraping Details"):
                data = future.result()
                if data:
                    results.append(data)

        if results:
            return pd.json_normalize(results)
        else:
            return pd.DataFrame()
