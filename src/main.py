from helper import *
from links import SrealityScraper
from detail import DetailScraper

if __name__ == "__main__":
    filters = {
        'category_main_cb': 1,
        'category_type_cb': 1,
        'locality_region_id': 10,
    }

    scraper = SrealityScraper(max_workers=5, filters=filters)
    timestamp = pd.Timestamp.now().isoformat()

    links_data = scraper.scrape_all_links_with_filter(
        timestamp=timestamp,
        per_page=320,
        max_pages=0
    )

    logger.info(f"Step 1 Complete: Found {len(links_data)} properties.")
    with open("../out/list_output.json", "w") as f:
        json.dump(links_data, f, indent=2)

    if links_data:
        detail_scraper = DetailScraper(max_workers=10)
        df_details = detail_scraper.fetch_details_batch(links_data)

        output_filename = f"../out/sreality_details_{int(time.time())}.csv"
        df_details.to_csv(output_filename, index=False, encoding='utf-8-sig')

        logger.info(f"Step 2 Complete: Details saved to {output_filename}")
        print(f"Successfully scraped {len(df_details)} details into {output_filename}")
    else:
        logger.warning("No links found in Step 1. Skipping Step 2.")
