# src/sentiment_analysis/sentiment_analysis.py
import os
import json
import sys
import time
import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor

# Path insertion
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- Scrapers ---
try:
    from src.sentiment_analysis.scrapers.reddit_scraper import RedditScraper
    from src.sentiment_analysis.scrapers.twitter_scraper import TwitterScraper
    from src.sentiment_analysis.scrapers.crypto_panic_news_scraper import CryptoPanicNewsScraper
except ImportError as e:
    import logging as temp_logging
    temp_logging.basicConfig(level=temp_logging.ERROR)
    temp_logging.error(f"Critical import error for scrapers: {e}. Some functionalities might be unavailable.")
    
# --- Sentiment Utilities ---
try:
    from src.sentiment_analysis.sentiment_utils import hybrid_sentiment
except ImportError:
    import logging as temp_logging
    temp_logging.basicConfig(level=temp_logging.ERROR)
    temp_logging.critical("Failed to import hybrid_sentiment from sentiment_utils. Sentiment analysis will not work.", exc_info=True)
    def hybrid_sentiment(text: str) -> Dict[str, Any]:
        temp_logging.error("hybrid_sentiment function is not available due to import error.")
        return {"score": 0.0, "category": "neutral", "error": "hybrid_sentiment not imported"}

# --- Project Utilities ---
try:
    from utils.logger import get_logger
    logger = get_logger("SentimentAnalysis")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("SentimentAnalysis_Fallback")
    logger.warning("Custom logger not found, using basic logging for SentimentAnalysis.")

try:
    from utils.credentials_loader import load_credentials
except ImportError:
    logger.error("Could not import config_loader. LLM API key loading from file will fail.")
    load_credentials = None # type: ignore


# --- LLM API Key Configuration ---
OPENROUTER_API_KEY_ENV_VAR = "OPENROUTER_API_KEY"
_llm_api_key: Optional[str] = None
_credentials_loaded: bool = False 

def _get_openrouter_api_key() -> Optional[str]:
    global _llm_api_key, _credentials_loaded
    
    if _llm_api_key is None:
        _llm_api_key = os.environ.get(OPENROUTER_API_KEY_ENV_VAR)
        if _llm_api_key:
            logger.info("OpenRouter API key loaded from environment variable.")
            return _llm_api_key

        if not _credentials_loaded and load_credentials: 
            credentials = load_credentials() 
            if credentials:
                _llm_api_key = credentials.get('openrouter', {}).get('api_key')
                if _llm_api_key:
                    logger.info("OpenRouter API key loaded from credentials.yaml.")
                else:
                    logger.warning("OpenRouter API key not found in credentials.yaml.")
            else:
                logger.warning("Failed to load credentials from file.")
            _credentials_loaded = True 

    if _llm_api_key is None:
        logger.error(f"{OPENROUTER_API_KEY_ENV_VAR} not found. LLM functions may be skipped.")
    return _llm_api_key


# --- Constants ---
CONFIG_PATH = Path(PROJECT_ROOT) / "config" / "credentials.yaml" 
SENTIMENT_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "sentiment")


def retry_request(func, *args, retries: int = 3, delay: int = 2, **kwargs) -> Any:
    """Helper function to retry network-bound functions."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{retries} failed for {func.__name__}: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1)) 
            else:
                logger.error(f"Function {func.__name__} failed after {retries} attempts: {e}", exc_info=True)
                if func.__name__ in ["scrape", "fetch_news"]:
                    return [] 
                return None 
    return None


def analyze_posts(posts: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
    """
    Analyzes a list of posts for sentiment using the LLM hybrid_sentiment function.
    """
    analyzed: List[Dict[str, Any]] = []
    if not posts:
        logger.info(f"No posts from {source} to analyze.")
        return analyzed
        
    for post_idx, post in enumerate(posts): 
        content = post.get("content", "") 
        if not isinstance(content, str):
            content = "" 
            
        sentiment_details: Dict[str, Any] 

        if not content.strip(): 
            sentiment_details = {"score": 0.0, "category": "neutral", "error": "Empty content"}
        else:
            sentiment_details = hybrid_sentiment(content) 

        if sentiment_details.get("error"):
            logger.warning(f"Sentiment analysis error for post {post_idx+1} from {source}: {sentiment_details['error']}.")
        
        post["sentiment_score"] = sentiment_details.get("score", 0.0)
        post["sentiment_category"] = str(sentiment_details.get("category", "neutral")).lower() 
        post["sentiment_analysis_details"] = sentiment_details 
        post["source"] = source 
        
        analyzed.append(post)

    logger.info(f"Analyzed {len(analyzed)} posts from {source}.")
    return analyzed


def save_results(data: Dict[str, Any], output_path: str) -> None:
    """Saves the analysis results to a JSON file."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Sentiment results saved to: {output_path}")
    except Exception as e:
        logger.exception(f"Failed to save sentiment results to {output_path}: {e}")


async def fetch_social_and_news_posts(coin_name: str, config_path_for_scrapers: str) -> tuple[List[Dict], List[Dict], List[Dict]]:
    """Fetches posts from Reddit, Twitter, and CryptoPanic concurrently."""
    loop = asyncio.get_running_loop()

    try:
        reddit_scraper = RedditScraper(config_path=config_path_for_scrapers)
        twitter_scraper = TwitterScraper(config_path=config_path_for_scrapers)
        cryptopanic_scraper = CryptoPanicNewsScraper(config_path=config_path_for_scrapers)
    except Exception as e: 
        logger.error(f"Error initializing scrapers: {e}. Using default configs.")
        reddit_scraper = RedditScraper() 
        twitter_scraper = TwitterScraper()
        cryptopanic_scraper = CryptoPanicNewsScraper()

    scraper_tasks = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        if hasattr(reddit_scraper, 'scrape') and callable(reddit_scraper.scrape):
            scraper_tasks.append(loop.run_in_executor(executor, retry_request, reddit_scraper.scrape, coin_name, 20))
        else: 
            scraper_tasks.append(asyncio.sleep(0, result=[]))
            
        if hasattr(twitter_scraper, 'scrape') and callable(twitter_scraper.scrape):
            scraper_tasks.append(loop.run_in_executor(executor, retry_request, twitter_scraper.scrape, coin_name, 20))
        else: 
            scraper_tasks.append(asyncio.sleep(0, result=[]))

        if hasattr(cryptopanic_scraper, 'fetch_news') and callable(cryptopanic_scraper.fetch_news):
            scraper_tasks.append(loop.run_in_executor(executor, retry_request, cryptopanic_scraper.fetch_news, coin_name, 20))
        else: 
            scraper_tasks.append(asyncio.sleep(0, result=[]))
        
        results = await asyncio.gather(*scraper_tasks, return_exceptions=True)

    processed_results = []
    for i, res in enumerate(results):
        if isinstance(res, Exception) or res is None:
            processed_results.append([]) 
        else:
            processed_results.append(res if isinstance(res, list) else [])
    
    reddit_posts_raw = processed_results[0] if len(processed_results) > 0 else []
    twitter_posts_raw = processed_results[1] if len(processed_results) > 1 else []
    cmc_articles_raw = processed_results[2] if len(processed_results) > 2 else []
    
    return reddit_posts_raw, twitter_posts_raw, cmc_articles_raw


async def get_sentiment_snapshot(symbol_or_coin_name: str) -> Dict[str, Any]:
    """
    Generates a fast, localized sentiment snapshot for a cryptocurrency.
    Fetches strictly from social media and crypto news, analyzes via LLM, and aggregates.
    """
    coin_name_for_scrapers = symbol_or_coin_name.lower() 
    logger.info(f"Starting sentiment snapshot for: {symbol_or_coin_name}")
    start_time_total = time.time()

    try:
        # --- 1. Fetch Social Media & Crypto News ---
        fetch_posts_start_time = time.time()
        reddit_posts_raw, twitter_posts_raw, cmc_articles_raw = await fetch_social_and_news_posts(coin_name_for_scrapers, CONFIG_PATH)
        logger.info(f"Fetched social/news posts in {time.time() - fetch_posts_start_time:.2f}s. "
                    f"Reddit: {len(reddit_posts_raw)}, Twitter: {len(twitter_posts_raw)}, CryptoPanic: {len(cmc_articles_raw)}")

        # --- 2. Analyze Sentiment via LLM ---
        logger.info("Analyzing sentiments for social media and CryptoPanic posts...")
        analyzed_posts_combined: List[Dict[str, Any]] = []

        analyzed_posts_combined.extend(analyze_posts(reddit_posts_raw, "reddit"))
        analyzed_posts_combined.extend(analyze_posts(twitter_posts_raw, "twitter"))
        
        processed_cmc_articles_for_sentiment: List[Dict[str, Any]] = []
        for article in cmc_articles_raw:
            title = article.get("title", "")
            summary_or_slug = article.get("summary", article.get("slug", "")) 
            article_copy = article.copy() 
            article_copy["content"] = f"{title}. {summary_or_slug}".strip() 
            processed_cmc_articles_for_sentiment.append(article_copy)
            
        analyzed_posts_combined.extend(analyze_posts(processed_cmc_articles_for_sentiment, "cryptopanic"))
        
        # Calculate average score
        valid_social_scores = [p.get("sentiment_score") for p in analyzed_posts_combined if isinstance(p.get("sentiment_score"), (int, float))]
        average_social_sentiment_score = sum(valid_social_scores) / len(valid_social_scores) if valid_social_scores else 0.0

        overall_social_category = "neutral"
        if average_social_sentiment_score > 0.5: overall_social_category = "bullish"
        elif average_social_sentiment_score > 0.1: overall_social_category = "mini bullish"
        elif average_social_sentiment_score < -0.5: overall_social_category = "bearish"
        elif average_social_sentiment_score < -0.1: overall_social_category = "mini bearish"

        # --- 3. Construct Final Result ---
        result = {
            "snapshot_timestamp_utc": datetime.utcnow().isoformat(),
            "symbol_analyzed": symbol_or_coin_name,
            "average_social_sentiment_score": round(average_social_sentiment_score, 4),
            "overall_social_sentiment_category": overall_social_category,
            "total_social_posts_analyzed": len(analyzed_posts_combined), 
            "source_breakdown": {
                "reddit_posts_fetched": len(reddit_posts_raw),
                "twitter_tweets_fetched": len(twitter_posts_raw),
                "cryptopanic_articles_fetched": len(cmc_articles_raw)
            }
        }

        # --- 4. Save to Disk ---
        output_filename = f"{symbol_or_coin_name.replace('/', '_')}_sentiment_snapshot_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        output_path = os.path.join(SENTIMENT_DATA_DIR, output_filename)
        save_results(result, output_path)

        logger.info(f"Sentiment snapshot for {symbol_or_coin_name} completed in {time.time() - start_time_total:.2f} seconds.")
        return result

    except Exception as e:
        logger.exception(f"Error generating sentiment snapshot for {symbol_or_coin_name}: {e}")
        return {
            "error": str(e),
            "symbol_analyzed": symbol_or_coin_name,
            "snapshot_timestamp_utc": datetime.utcnow().isoformat(),
            "average_social_sentiment_score": 0.0,
            "overall_social_sentiment_category": "neutral",
            "total_social_posts_analyzed": 0,
            "source_breakdown": {"reddit_posts_fetched": 0, "twitter_tweets_fetched": 0, "cryptopanic_articles_fetched": 0}
        }


async def run_sentiment_analysis_for_symbol(symbol: str) -> Dict[str, Any]:
    """Helper function to run sentiment analysis for a single symbol."""
    logger.info(f"Queuing sentiment analysis for symbol: {symbol}")
    return await get_sentiment_snapshot(symbol)


if __name__ == "__main__":
    test_symbol_input = "BTC"  
    
    loaded_api_key = _get_openrouter_api_key()
    if not loaded_api_key:
        print("\nWARNING: OpenRouter API key could not be loaded. LLM-based sentiment analysis may fail.")

    if not callable(hybrid_sentiment) or hybrid_sentiment("test").get("error") == "hybrid_sentiment not imported":
        print("\nCRITICAL: `hybrid_sentiment` could not be imported correctly. Sentiment analysis will fail.")
    else:
        logger.info("`hybrid_sentiment` appears to be imported correctly.")

    print(f"\nRunning sentiment analysis for: {test_symbol_input}...")
    
    try:
        final_result_for_test = asyncio.run(run_sentiment_analysis_for_symbol(test_symbol_input))
        
        print("\n--- Sentiment Analysis Result (Summary) ---")
        if final_result_for_test.get("error"):
            print(f"Error: {final_result_for_test['error']}")
        else:
            print(f"Symbol Analyzed: {final_result_for_test.get('symbol_analyzed')}")
            print(f"Timestamp UTC: {final_result_for_test.get('snapshot_timestamp_utc')}")
            print(f"Avg. Social Sentiment Score: {final_result_for_test.get('average_social_sentiment_score')}")
            print(f"Overall Social Sentiment Category: {final_result_for_test.get('overall_social_sentiment_category')}")
            print(f"Total Social Posts Analyzed: {final_result_for_test.get('total_social_posts_analyzed')}")

    except Exception as e:
        print(f"\nAn unexpected error occurred in the main test execution: {e}")

    print("\n--- End of Test Run ---")
