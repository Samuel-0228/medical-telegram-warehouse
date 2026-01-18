# Interim Report: Medical Telegram Warehouse

## Data Lake Structure
- **Partitioning**: JSON files in `data/raw/telegram_messages/
- **Images**: `data/raw/images/{channel_name}/{message_id}.jpg` for direct linking.
- **Logs**: `logs/scraper_*.log` tracks errors/rates (e.g., 150 msgs scraped, 0 errors).
- **Rationale**: Atomic, immutable raw zone for reproducibility; no transformations here.

## Star Schema Diagram
(Use draw.io or text; embed PNG in GitHub README later.)

Fact: fct_messages ← dim_channels (channel_key), dim_dates (date_key)

- dim_channels: Channel metadata (key, name, type, aggregates).
- dim_dates: Time hierarchy for trends.
- fct_messages: Granular events (text, metrics).

ASCII Diagram: