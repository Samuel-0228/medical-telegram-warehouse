# api/main.py
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from .database import SessionLocal, engine
from .schemas import MessageResponse, TopProductResponse, ActivityResponse
from typing import List

app = FastAPI(title="Medical Telegram API",
              description="Analytical endpoints for Ethiopian pharma insights")

# Dependency


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/api/reports/top-products", response_model=List[TopProductResponse])
def top_products(limit: int = 10, db: Session = Depends(get_db)):
    query = text("""
        SELECT lower(regexp_split_to_table(message_text, '\\s+')) as product,
               COUNT(*) as mention_count
        FROM fct_messages
        WHERE message_text ILIKE '%paracetamol%' OR message_text ILIKE '%drug%'  -- Simple keyword filter; enhance with NLP later
        GROUP BY 1 ORDER BY 2 DESC LIMIT :limit
    """)
    result = db.execute(query, {"limit": limit}).fetchall()
    return [{"product": row[0], "mention_count": row[1]} for row in result]


@app.get("/api/channels/{channel_name}/activity", response_model=List[ActivityResponse])
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    query = text("""
        SELECT dd.full_date::text as date_key, COUNT(*) as post_count, SUM(fm.view_count) as total_views
        FROM fct_messages fm
        JOIN dim_channels dc ON fm.channel_key = dc.channel_key
        JOIN dim_dates dd ON fm.date_key = dd.date_key
        WHERE dc.channel_name ILIKE :channel_name
        GROUP BY dd.full_date ORDER BY dd.full_date DESC
    """)
    result = db.execute(
        query, {"channel_name": f"%{channel_name}%"}).fetchall()
    if not result:
        raise HTTPException(status_code=404, detail="Channel not found")
    return [{"date_key": row[0], "post_count": row[1], "total_views": row[2]} for row in result]


@app.get("/api/search/messages", response_model=List[MessageResponse])
def search_messages(query: str, limit: int = 20, db: Session = Depends(get_db)):
    query_sql = text("""
        SELECT message_id, message_text, view_count
        FROM fct_messages
        WHERE message_text ILIKE :query
        ORDER BY view_count DESC LIMIT :limit
    """)
    result = db.execute(
        query_sql, {"query": f"%{query}%", "limit": limit}).fetchall()
    return [MessageResponse(**{k: v for k, v in zip(['message_id', 'message_text', 'view_count'], row)})} for row in result]

    @ app.get("/api/reports/visual-content")
        def visual_stats(db: Session=Depends(get_db)):
    query = text("""
        SELECT dc.channel_name, fid.image_category, COUNT(*) as count, AVG(fid.view_count) as avg_views
        FROM fct_image_detections fid
        JOIN dim_channels dc ON fid.channel_key = dc.channel_key
        GROUP BY dc.channel_name, fid.image_category
        ORDER BY dc.channel_name, count DESC
    """)
        result = db.execute(query).fetchall()
    return [{"channel": row[0], "category": row[1], "count": row[2], "avg_views": row[3]} for row in result]

        if __name__ == "__main__":
    import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
