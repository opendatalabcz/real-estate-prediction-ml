from helper import *


class SrealityScraper:
    def __init__(self, max_workers: int = 5, filters: Dict[str, Any] = None):
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
        self.filters = filters

    def set_filters(self, filters: Dict[str, Any]):
        self.filters = filters

    def get_total_count(self) -> int:
        url = "https://www.sreality.cz/api/cs/v2/estates/count"
        params = self.filters if self.filters else {}
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json().get("result_size", 0)
        except Exception as e:
            logger.error(f"Error getting count: {e}")
            return 0

    def generate_urls(self, per_page: int = 60, max_pages: Optional[int] = None) -> List[str]:
        total_count = self.get_total_count()
        logger.info(f'Total estates found: {total_count}')

        if max_pages:
            pages = min(max_pages, (total_count // per_page) + 1)
        else:
            pages = (total_count // per_page) + 1

        logger.info(f'Preparing to scrape {pages} pages')

        urls = []
        api_url = "https://www.sreality.cz/api/cs/v2/estates"

        for page in range(1, pages + 1):
            params = self.filters.copy()
            params.update({
                'per_page': per_page,
                'page': page,
                'tms': int(time.time() * 1000)
            })
            query_string = '&'.join([f'{k}={v}' for k, v in params.items()])
            urls.append(f"{api_url}?{query_string}")

        return urls

    def scrape_estates_batch(self, urls: List[str]) -> List[Dict[str, Any]]:
        all_estates = []

        def fetch_page(url: str) -> Optional[List[Dict]]:
            try:
                time.sleep(random.uniform(0.1, 0.5))
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                return response.json().get('_embedded', {}).get('estates', [])
            except Exception as e:
                logger.error(f"Error fetching page: {e}")
                return None

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(fetch_page, url): url for url in urls}

            for future in tqdm(as_completed(future_to_url), total=len(urls), desc="Scraping List Pages"):
                estates = future.result()
                if estates:
                    all_estates.extend(estates)

        return all_estates

    def scrape_all_links_with_filter(self, timestamp: str, per_page: int = 60, max_pages: Optional[int] = None, save_intermediate: bool = True):
        urls = self.generate_urls(per_page=per_page, max_pages=max_pages)

        if not urls:
            return []

        all_estates = self.scrape_estates_batch(urls)
        data = []
        api_url = "https://www.sreality.cz/api/cs/v2/estates"

        for estate in all_estates:
            try:
                seo = estate.get("seo", {})
                type_id = seo.get("category_type_cb")
                main_id = seo.get("category_main_cb")
                sub_id = seo.get("category_sub_cb")

                cat_type_str = category_type_to_url.get(type_id, "neurcito")
                cat_main_str = category_main_to_url.get(main_id, "neurcito")
                cat_sub_str = category_sub_to_url.get(sub_id, "neurcito")
                locality = seo.get("locality", "")
                hash_id = estate.get("hash_id", "")

                link = f"https://www.sreality.cz/detail/{cat_type_str}/{cat_main_str}/{cat_sub_str}/{locality}/{hash_id}"
                api_link = f"{api_url}/{hash_id}"

                # Appending tuple: (api_link, public_web_link)
                data.append((api_link, link))
            except Exception as e:
                logger.warning(f"Error processing estate in list view: {e}")
                continue

        return data
