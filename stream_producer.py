# stream_producer.py
import csv
import json
import time
import random
from azure.eventhub import EventHubProducerClient, EventData

# ── CONNECTION ─────────────────────────────────────────────
CONNECTION_STR = "Endpoint=sb://eseham8hii5dkopcxlrh4h.servicebus.windows.net/;SharedAccessKeyName=key_9700ff52-1e8c-456a-b12d-317456bcec8f;SharedAccessKey=dqeRLv0mjY0gAdlDk9Y27Rt1iclfxHmLm+AEhAtiq38="
EVENTHUB_NAME  = "es_cf85ce93-8fbd-45a5-b9c3-1536c7287734"
CSV_FILE       = "hotel_raw_stream.csv"
DELAY_SECONDS  = 0.5  # her event arasında bekleme süresi

# ── PRODUCER ──────────────────────────────────────────────
def send_events():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )

    with producer:
        with open(CSV_FILE, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                event_data = EventData(json.dumps(row))
                event_batch = producer.create_batch()
                event_batch.add(event_data)
                producer.send_batch(event_batch)

                if i % 100 == 0:
                    print(f"✅ {i} event gönderildi...")

                time.sleep(DELAY_SECONDS)

    print("🎉 Tüm eventler gönderildi!")

if __name__ == "__main__":
    send_events()
