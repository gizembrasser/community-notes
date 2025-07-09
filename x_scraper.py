import os
import json
import asyncio
from dotenv import load_dotenv, find_dotenv
from scrapfly import ScrapeConfig, ScrapflyClient
import logging
import asyncio
import pandas as pd

# Load API key from environment variables
load_dotenv(find_dotenv())
key = os.getenv("SCRAPFLY_KEY")

SCRAPFLY = ScrapflyClient(key=key)


async def scrape_tweet(url, delay=3, max_retries=3):
    """Scrape tweet text and time from a URL with retry and delay to respect rate limits."""
    for attempt in range(max_retries):
        try:
            await asyncio.sleep(delay)  # Throttle requests
            result = await SCRAPFLY.async_scrape(ScrapeConfig(
                url,
                render_js=True, # Enable headless browser
                wait_for_selector="[data-testid='tweet']" # Wait for page to finish loading
            ))  

            # Capture background requests and extract ones that request tweet data   
            xhr_calls = result.scrape_result["browser_data"]["xhr_call"]
            tweet_call = [f for f in xhr_calls if "TweetResultByRestId" in f["url"]]

            for xhr in tweet_call:
                if not xhr["response"]:
                    continue

                data = json.loads(xhr["response"]["body"])
                legacy = data['data']['tweetResult']['result']['legacy']

                # Return the full text and timestamp of the tweet
                return legacy['full_text'], legacy['created_at']
        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt == max_retries - 1:
                return None, None
            await asyncio.sleep(2 ** attempt)  # Exponential backoff


async def enrich_df(df):
    """Enrich a DataFrame containing tweet URLs with tweet text and timestamp."""
    texts = []
    timestamps = []

    for i, url in enumerate(df["sourceLink"].to_list()):
        print(f"Scraping {i+1}/{len(df)}: {url}")

        text, timestamp = await scrape_tweet(url)
        texts.append(text)
        timestamps.append(timestamp)
    
    df["source_text"] = texts
    df["source_timestamp"] = timestamps
    return df


df = pd.read_csv("data/notes/helpful_notes.csv")
enriched_df = asyncio.run(enrich_df(df))

# Save result
enriched_df.to_csv("data/notes/helpful_notes_enriched.csv", index=False)
print("Enrichment process done.")


