# src/scraper.py
import os
import json
import logging
from datetime import datetime
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto
import asyncio
from dotenv import load_dotenv

load_dotenv()

# Config
API_ID = int(os.getenv('TELEGRAM_API_ID'))
API_HASH = os.getenv('TELEGRAM_API_HASH')
CHANNELS = ['chemed_et', 'lobelia4cosmetics', 'tikvahpharma',
            'ethiopianpharmacy']  # Add more as needed
DATA_DIR = 'data/raw/telegram_messages'
IMAGES_DIR = 'data/raw/images'
LOGS_DIR = 'logs'
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(
        LOGS_DIR, f'scraper_{datetime.now().strftime("%Y%m%d")}.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


async def scrape_channel(client, channel, limit=200):
    """Scrape messages from a channel."""
    try:
        entity = await client.get_entity(channel)
        channel_name = entity.title or channel
        logging.info(f"Starting scrape for {channel_name}")

        messages = []
        async for message in client.iter_messages(entity, limit=limit):
            msg_data = {
                'message_id': message.id,
                'channel_name': channel_name,
                'message_date': message.date.isoformat(),
                'message_text': message.message or '',
                'has_media': bool(message.media),
                'views': getattr(message, 'views', 0),
                'forwards': getattr(message, 'forwards', {}).total_count if hasattr(message, 'forwards') else 0
            }

            # Download image if present
            image_path = None
            if isinstance(message.media, MessageMediaPhoto):
                image_filename = f"{channel_name}/{message.id}.jpg"
                image_path = os.path.join(IMAGES_DIR, image_filename)
                os.makedirs(os.path.dirname(image_path), exist_ok=True)
                await client.download_media(message.media, image_path)
                msg_data['image_path'] = image_path

            messages.append(msg_data)

        # Save partitioned JSON
        # Or use message dates for partitioning
        date_str = datetime.now().strftime("%Y-%m-%d")
        partition_dir = os.path.join(DATA_DIR, date_str, channel_name)
        os.makedirs(partition_dir, exist_ok=True)
        json_path = os.path.join(partition_dir, f"{channel_name}.json")
        with open(json_path, 'w') as f:
            json.dump(messages, f, indent=2)

        logging.info(
            f"Scraped {len(messages)} messages from {channel_name}, saved to {json_path}")
        return len(messages)

    except Exception as e:
        logging.error(f"Error scraping {channel}: {str(e)}")
        return 0


async def main():
    client = TelegramClient('session', API_ID, API_HASH)
    await client.start()

    total_scraped = 0
    for channel in CHANNELS:
        total_scraped += await scrape_channel(client, channel)

    await client.disconnect()
    logging.info(f"Total messages scraped: {total_scraped}")

if __name__ == "__main__":
    asyncio.run(main())
