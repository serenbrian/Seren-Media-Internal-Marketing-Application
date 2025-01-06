import requests
import json
import time
from datetime import datetime, timedelta
import asyncio
import aiohttp
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, TypedDict, Union
from notion_client import AsyncClient

# Logging configuration
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('social_analytics.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('SocialAnalytics')

class RateLimiter:
    def __init__(self, calls_per_second=2, max_calls_per_minute=100):
        self.calls_per_second = calls_per_second
        self.max_calls_per_minute = max_calls_per_minute
        self.timestamps_sec = []
        self.timestamps_min = []

    async def wait(self):
        now = time.time()
        # Clean up timestamps older than 1 second and 60 seconds
        self.timestamps_sec = [ts for ts in self.timestamps_sec if now - ts < 1.0]
        self.timestamps_min = [ts for ts in self.timestamps_min if now - ts < 60.0]

        if len(self.timestamps_sec) >= self.calls_per_second or len(self.timestamps_min) >= self.max_calls_per_minute:
            sleep_time = 0
            if len(self.timestamps_sec) >= self.calls_per_second:
                sleep_time = 1 - (now - self.timestamps_sec[0])
            elif len(self.timestamps_min) >= self.max_calls_per_minute:
                sleep_time = 60 - (now - self.timestamps_min[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        self.timestamps_sec.append(now)
        self.timestamps_min.append(now)

class PostData(TypedDict):
    id: Optional[str]
    content: Optional[str]
    created_time: str
    engagement: float
    impressions: Optional[int]
    reach: Optional[int]
    type: str
    url: Optional[str]
    media_url: Optional[str]

@dataclass
class Config:
    metricool_token: str = 'BAMWHTBZCVACVCHRVWXWBBFTZJLKWKZVZTQQRGVZWHPWJMTUVJVPCOJVPDHAISMM'
    metricool_user_id: str = '3377912'
    metricool_blog_id: str = '4308896'
    notion_token: str = "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"
    notion_database_id: str = "16ec9dd35920819cbc30f4d1574f0f8e"  # Your existing Notion database ID
    metricool_base_url: str = 'https://app.metricool.com/api'
    notion_base_url: str = "https://api.notion.com/v1"
    notion_version: str = "2022-06-28"

    def get_metricool_headers(self):
        return {
            'X-Mc-Auth': self.metricool_token,
            'Content-Type': 'application/json'
        }

    def get_notion_headers(self):
        return {
            "Authorization": f"Bearer {self.notion_token}",
            "Content-Type": "application/json",
            "Notion-Version": self.notion_version
        }

class MetricoolAPI:
    def __init__(self, config: Config, session: aiohttp.ClientSession):
        self.config = config
        self.session = session
        self.rate_limiter = RateLimiter(calls_per_second=2)

    async def fetch_data(self, endpoint: str, params: dict = None) -> dict:
        if params is None:
            params = {}

        auth_params = {
            'userId': self.config.metricool_user_id,
            'blogId': self.config.metricool_blog_id
        }
        params.update(auth_params)

        url = f"{self.config.metricool_base_url}{endpoint}"
        headers = self.config.get_metricool_headers()

        try:
            logger.debug(f"Fetching from {url} with params: {params}")
            await self.rate_limiter.wait()
            
            async with self.session.get(
                url,
                params=params,
                headers=headers
            ) as response:
                if response.status == 429:  # Too Many Requests
                    retry_after = float(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    return await self.fetch_data(endpoint, params)
                elif response.status == 204:  # No Content
                    logger.info(f"No content available for {endpoint}")
                    return []

                response.raise_for_status()
                data = await response.json()
                logger.debug(f"Response data: {data}")
                
                return self._process_response(data)
                
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching from {endpoint}: {str(e)}")
            if 'response' in locals():
                logger.error(f"Response status: {response.status}")
                logger.error(f"Response text: {await response.text()}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error from {endpoint}: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching from {endpoint}: {str(e)}")
            return []

    def _process_response(self, data: Union[List, Dict]) -> List:
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get('data', [])
        return []

    def validate_post_data(self, data: dict, platform: str) -> Optional[PostData]:
        try:
            # Different platforms use different field names for IDs and timestamps
            id_fields = ['id', 'postId', 'videoId', 'postUrl', 'mediaId']  # Added 'postUrl' and 'mediaId'
            time_fields = ['created_time', 'created', 'timestamp', 'createdTime', 'publishedAt', 'createTime']
            content_fields = ['message', 'caption', 'text', 'description', 'videoDescription']
            url_fields = ['permalink', 'url', 'shareUrl', 'watchUrl']
            media_fields = ['picture', 'mediaUrl', 'thumbnailUrl', 'coverImageUrl']

            # Find the ID field
            post_id = None
            for field in id_fields:
                if field in data and data[field]:
                    post_id = data[field]
                    break
            
            # If no ID found, skip the item
            if not post_id:
                logger.warning(f"Missing 'id' and alternative ID fields for {platform}")
                logger.debug(f"Incomplete data for {platform}: {json.dumps(data, indent=2)}")
                return None

            # Find the timestamp field
            created_time = None
            for field in time_fields:
                if field in data and data[field]:
                    created_time = data[field]
                    break

            if not created_time:
                logger.warning(f"Missing timestamp field for {platform}")
                logger.debug(f"Incomplete data for {platform}: {json.dumps(data, indent=2)}")
                return None

            # Find content
            content = None
            for field in content_fields:
                if field in data and data[field]:
                    content = data[field]
                    break

            # Find URL
            url = None
            for field in url_fields:
                if field in data and data[field]:
                    url = data[field]
                    break

            # Find media URL
            media_url = None
            for field in media_fields:
                if field in data and data[field]:
                    media_url = data[field]
                    break

            # Handle different engagement metrics by platform
            engagement = 0.0
            if platform == 'youtube':
                # For YouTube, calculate engagement from available metrics
                views = float(data.get('views', 0))
                likes = float(data.get('likes', 0))
                comments = float(data.get('comments', 0))
                if views > 0:
                    engagement = ((likes + comments) / views) * 100
            elif platform == 'tiktok':
                # For TikTok, use engagement or calculate from interactions
                views = float(data.get('viewCount', 0))
                likes = float(data.get('likeCount', 0))
                comments = float(data.get('commentCount', 0))
                shares = float(data.get('shareCount', 0))
                if views > 0:
                    engagement = ((likes + comments + shares) / views) * 100
            else:
                engagement = float(data.get('engagement', 0))

            return PostData(
                id=str(post_id),
                content=content or '',
                created_time=created_time,
                engagement=engagement,
                impressions=data.get('impressions', data.get('impressionsTotal')),
                reach=data.get('reach', data.get('viewCount')),
                type=data.get('type', data.get('mediaType', 'post')),
                url=url,
                media_url=media_url
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Data validation error for {platform}: {str(e)}")
            return None

    async def fetch_platform_data(self, platform: str, start_date: str, end_date: str) -> list:
        endpoint_map = {
            'facebook': '/stats/facebook/posts',
            'instagram': '/stats/instagram/posts',
            'linkedin': '/stats/linkedin/posts',
            'twitter': '/stats/twitter/posts',
            'youtube': '/v2/analytics/posts/youtube',
            'tiktok': '/v2/analytics/posts/tiktok'
        }

        if platform not in endpoint_map:
            logger.warning(f"Platform {platform} not supported")
            return []

        start = start_date.replace('-', '')
        end = end_date.replace('-', '')

        params = {
            'start': start,
            'end': end,
            'timezone': 'UTC'
        }

        # Add platform specific params
        platform_params = {
            'facebook': {
                'type': 'all',
                'sortcolumn': 'reactions,engagement,shares,impressions,impressionsUnique,clicks,linkclicks,comments,videoViews,videoTimeWatched'
            },
            'instagram': {
                'includeStories': 'true',
                'sortcolumn': 'engagement,impressions,reach,likes,comments,saves'
            },
            'linkedin': {
                'type': 'all',
                'sortcolumn': 'likes,clicks,impressions,engagement,comments'
            },
            'twitter': {
                'includeReplies': 'true',
                'sortcolumn': 'engagement,impressions,retweets,replies,likes'
            },
            'youtube': {
                'sortcolumn': 'views,comments,likes,engagement'
            },
            'tiktok': {
                'sortcolumn': 'viewCount,likeCount,commentCount,shareCount'
            }
        }

        # For v2 endpoints, use ISO format dates
        if platform in ['youtube', 'tiktok']:
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            params['from'] = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
            params['to'] = end_dt.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            params['start'] = start
            params['end'] = end

        if platform in platform_params:
            params.update(platform_params[platform])

        try:
            logger.info(f"Fetching {platform} data from {start} to {end}")
            data = await self.fetch_data(endpoint_map[platform], params)

            validated_data = []
            for item in data:
                validated_item = self.validate_post_data(item, platform)
                if validated_item:
                    validated_data.append(validated_item)

            if validated_data:
                logger.info(f"Retrieved and validated {len(validated_data)} items for {platform}")
                return validated_data
            else:
                logger.warning(f"No valid data retrieved for {platform}")
                return []

        except Exception as e:
            logger.error(f"Error fetching {platform} data: {str(e)}")
            return []

class NotionAPI:
    def __init__(self, config: Config):
        self.notion = AsyncClient(auth=config.notion_token)
        self.database_id = config.notion_database_id
        self.rate_limiter = RateLimiter(calls_per_second=3, max_calls_per_minute=100)
        self.existing_post_ids = set()  # To store existing Post IDs

    async def fetch_existing_post_ids(self):
        try:
            has_more = True
            next_cursor = None
            while has_more:
                await self.rate_limiter.wait()
                response = await self.notion.databases.query(
                    database_id=self.database_id,
                    page_size=100,
                    start_cursor=next_cursor
                )
                results = response.get('results', [])
                for page in results:
                    properties = page.get('properties', {})
                    post_id_property = properties.get('Post ID', {})
                    title = post_id_property.get('title', [])
                    if title:
                        existing_id = title[0].get('text', {}).get('content', '')
                        if existing_id:
                            self.existing_post_ids.add(existing_id)
                has_more = response.get('has_more', False)
                next_cursor = response.get('next_cursor', None)
            logger.info(f"Fetched {len(self.existing_post_ids)} existing Post IDs from Notion")
        except Exception as e:
            logger.error(f"Error fetching existing Post IDs: {str(e)}")

    async def add_items(self, database_id: str, items: List[Dict[str, Any]], batch_size: int = 10) -> int:
        success_count = 0
        try:
            # Fetch existing Post IDs before adding new items
            await self.fetch_existing_post_ids()

            # Track unique Post IDs within the current run to prevent duplicates
            unique_post_ids_run = set()

            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                
                for j, item in enumerate(batch):
                    post_id = item.get('Post ID', {}).get('title', [{}])[0].get('text', {}).get('content', '')
                    if not post_id:
                        logger.warning(f"Item {i+j+1} is missing 'Post ID'. Skipping.")
                        continue
                    if post_id in self.existing_post_ids or post_id in unique_post_ids_run:
                        logger.info(f"Skipping duplicate Post ID: {post_id}")
                        continue  # Skip duplicate
                    await self.rate_limiter.wait()
                    try:
                        added = await self._add_item(database_id, item, post_id)
                        if added:
                            success_count += 1
                            unique_post_ids_run.add(post_id)
                    except Exception as e:
                        logger.error(f"Error adding item {i+j+1}: {str(e)}")
                        continue
                
                # Rate limiting between batches
                await asyncio.sleep(0.5)
            
            return success_count
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            return success_count

    async def _add_item(self, database_id: str, item: Dict[str, Any], post_id: str) -> bool:
        max_retries = 3
        retry_delay = 1  # Start with 1 second

        for attempt in range(1, max_retries + 1):
            try:
                await self.notion.pages.create(
                    parent={"database_id": database_id},
                    properties=item
                )
                self.existing_post_ids.add(post_id)  # Add to existing IDs to prevent duplicates in the same run
                logger.info(f"Successfully added Post ID: {post_id}")
                return True
            except Exception as e:
                if 'Conflict' in str(e):
                    if attempt < max_retries:
                        logger.warning(f"Conflict detected for Post ID {post_id}. Retrying in {retry_delay} seconds... (Attempt {attempt}/{max_retries})")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        logger.error(f"Failed to add Post ID {post_id} after {max_retries} attempts due to conflicts.")
                        return False
                else:
                    logger.error(f"Error adding Post ID {post_id}: {str(e)}")
                    return False

def transform_item(platform: str, item: PostData) -> Optional[dict]:
    try:
        def safe_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value) if value is not None else default
            except (TypeError, ValueError):
                return default

        # Handle the date string conversion
        def format_date(date_value: Union[str, dict]) -> str:
            if isinstance(date_value, dict):
                # Handle dictionary format
                if 'dateTime' in date_value:
                    return date_value['dateTime']
            elif isinstance(date_value, str):
                # Handle direct ISO string
                return date_value
            # Return current time as fallback
            return datetime.utcnow().isoformat()

        base_properties = {
            "Post ID": {
                "title": [{"text": {"content": str(item['id'])}}]
            },
            "Platform": {
                "select": {"name": platform.lower()}
            },
            "Date": {
                "date": {
                    "start": format_date(item['created_time'])
                }
            },
            "Content": {
                "rich_text": [{
                    "text": {"content": str(item.get('content', ''))[:2000]}
                }]
            },
            "URL": {
                "url": item.get('url') or None
            },
            "Media URL": {
                "url": item.get('media_url') or None
            },
            "Media Type": {
                "select": {"name": item.get('type', 'post').lower()}
            },
            "Reach": {
                "number": safe_float(item.get('reach'))
            },
            "Impressions": {
                "number": safe_float(item.get('impressions'))
            },
            "Engagement Rate": {
                "number": safe_float(item.get('engagement'))
            },
            "Sync Date": {
                "date": {
                    "start": datetime.utcnow().isoformat()
                }
            }
        }

        # Remove None values to prevent Notion API errors
        # Ensure that 'url' properties are either present or omitted entirely
        clean_properties = {}
        for k, v in base_properties.items():
            if v is not None:
                if k in ['URL', 'Media URL'] and v.get('url') is None:
                    continue  # Skip if URL is None
                clean_properties[k] = v

        return clean_properties

    except (ValueError, TypeError) as e:
        logger.error(f"Data validation error for {platform}: {str(e)}")
        return None

async def main():
    config = Config()
    async with aiohttp.ClientSession() as session:
        try:
            metricool = MetricoolAPI(config, session)
            notion = NotionAPI(config)

            # Initialize date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)  # Initial start date (30 days ago)

            # Define platforms and initialize containers
            platforms = ['facebook', 'instagram', 'linkedin', 'youtube', 'tiktok']
            all_data = []
            failed_platforms = []
            unique_post_ids_all = set()

            # Define the maximum date back to fetch data
            # For example, fetch data up to 5 years back
            max_years_back = 5
            earliest_date = end_date - timedelta(days=365 * max_years_back)

            # Process each platform
            for platform in platforms:
                logger.info(f"Starting data collection for {platform}...")
                try:
                    platform_data = []
                    current_start = earliest_date
                    current_end = start_date

                    while current_start <= end_date:
                        formatted_start = current_start.strftime('%Y%m%d')
                        formatted_end = current_end.strftime('%Y%m%d')
                        logger.info(f"Fetching data for {platform} from {formatted_start} to {formatted_end}")
                        data = await metricool.fetch_platform_data(platform, formatted_start, formatted_end)
                        
                        if data:
                            logger.info(f"Successfully retrieved {len(data)} items for {platform} from {formatted_start} to {formatted_end}")
                            
                            # Transform data
                            transformed_items = []
                            for item in data:
                                try:
                                    transformed_item = transform_item(platform, item)
                                    if transformed_item:
                                        post_id = transformed_item.get('Post ID', {}).get('title', [{}])[0].get('text', {}).get('content', '')
                                        if post_id and post_id not in unique_post_ids_all:
                                            transformed_items.append(transformed_item)
                                            unique_post_ids_all.add(post_id)
                                        else:
                                            logger.info(f"Duplicate Post ID within run: {post_id}. Skipping.")
                                except Exception as e:
                                    logger.error(f"Error transforming item for {platform}: {str(e)}")
                                    continue
                            
                            if transformed_items:
                                platform_data.extend(transformed_items)
                                logger.info(f"Successfully transformed {len(transformed_items)} items for {platform}")
                            else:
                                logger.warning(f"No unique items were successfully transformed for {platform} in this batch")
                        else:
                            logger.info(f"No data retrieved for {platform} from {formatted_start} to {formatted_end}")

                        # Move the window back by 30 days
                        current_start += timedelta(days=30)
                        current_end += timedelta(days=30)
                        # To avoid fetching beyond the current end_date
                        if current_end > end_date:
                            current_end = end_date

                        # Optional: Add a short delay between requests to prevent hitting rate limits
                        await asyncio.sleep(0.1)

                    if platform_data:
                        all_data.extend(platform_data)
                        logger.info(f"Total items collected for {platform}: {len(platform_data)}")
                    else:
                        logger.warning(f"No data collected for {platform}")
                        failed_platforms.append(platform)
                
                except Exception as e:
                    logger.error(f"Failed to process {platform}: {str(e)}")
                    failed_platforms.append(platform)
                    continue

                # Rate limiting between platforms
                await asyncio.sleep(1)

            # Report collection summary
            logger.info("----- Data Collection Summary -----")
            logger.info(f"Total platforms processed: {len(platforms)}")
            logger.info(f"Successful platforms: {len(platforms) - len(failed_platforms)}")
            if failed_platforms:
                logger.warning(f"Failed platforms: {', '.join(failed_platforms)}")
            logger.info(f"Total items collected: {len(all_data)}")

            # Proceed with Notion data upload if we have data
            if all_data:
                logger.info(f"Preparing to add {len(all_data)} items to Notion...")

                try:
                    # Process items in batches
                    batch_size = 10
                    total_success = 0
                    
                    for i in range(0, len(all_data), batch_size):
                        batch = all_data[i:i + batch_size]
                        success_count = await notion.add_items(config.notion_database_id, batch, batch_size=batch_size)
                        total_success += success_count
                        logger.info(f"Batch progress: {min(i + batch_size, len(all_data))}/{len(all_data)} items processed")
                        await asyncio.sleep(0.5)  # Rate limiting between batches

                    # Report final results
                    logger.info("----- Data Upload Summary -----")
                    logger.info(f"Total items processed: {len(all_data)}")
                    logger.info(f"Successfully uploaded: {total_success}")
                    logger.info(f"Failed uploads: {len(all_data) - total_success}")
                    
                    if total_success < len(all_data):
                        logger.warning("Some items failed to upload to Notion")
                    else:
                        logger.info("All items successfully uploaded to Notion")

                except Exception as e:
                    logger.error(f"Error in Notion database operations: {str(e)}")
                    raise
            else:
                logger.warning("No data was collected from any platform")

        except Exception as e:
            logger.error(f"Critical error in main execution: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}")
        raise
