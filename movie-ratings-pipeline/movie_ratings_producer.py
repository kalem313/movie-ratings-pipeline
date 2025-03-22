import csv
import json
import random
import pandas as pd
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer


# Load movies from CSV
def load_movies(filepath):
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        return [row[0] for row in reader if row]

# Initialize
faker = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Create list of names
fake_names = [faker.name() for _ in range(10)]
my_name = "Kalem"
names = fake_names + [my_name]

# Load movie list
movies = load_movies("data/movies.csv")

topic = 'movie_ratings'

while True:
    for name in names:
        movie = random.choice(movies)
        rating = random.randint(1, 10)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message = {
            "name": name,
            "movie": movie,
            "timestamp": timestamp,
            "rating": rating
        }

        print(f"Sending: {message}")
        producer.send(topic, value=message)

    # Wait 60 seconds before next round
    time.sleep(60)
