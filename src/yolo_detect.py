# src/yolo_detect.py
import os
import csv
from ultralytics import YOLO
from PIL import Image
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

model = YOLO('yolov8n.pt')  # Pre-trained nano model
IMAGES_DIR = 'data/raw/images'
OUTPUT_CSV = 'data/yolo_detections.csv'

def classify_image(detections):
    """Simple classification based on YOLO labels."""
    has_person = any('person' in det['name'].lower() for det in detections)
    has_product = any(label in ['bottle', 'cup', 'bowl', 'vase', 'pill', 'box'] for det in detections for label in [det['name'].lower()])
    
    if has_person and has_product:
        return 'promotional'
    elif has_product:
        return 'product_display'
    elif has_person:
        return 'lifestyle'
    else:
        return 'other'

def detect_on_images():
    results = []
    for root, dirs, files in os.walk(IMAGES_DIR):
        for file in files:
            if file.endswith('.jpg'):
                image_path = os.path.join(root, file)
                channel = os.path.basename(root)
                msg_id = os.path.splitext(file)[0]
                
                # Run YOLO
                img = Image.open(image_path)
                preds = model(img)[0]
                detections = [{'name': cls['name'], 'confidence': cls['confidence']} for cls in preds.boxes.data.tolist()]  # Simplified
                
                category = classify_image(detections)
                results.append({
                    'message_id': int(msg_id),
                    'channel_name': channel,
                    'image_path': image_path,
                    'detected_objects': ', '.join([d['name'] for d in detections]),
                    'avg_confidence': sum(d['confidence'] for d in detections) / len(detections) if detections else 0,
                    'image_category': category
                })
    
    # Save CSV
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Detections saved: {len(results)} images processed.")

if __name__ == "__main__":
    detect_on_images()