import csv
import random

HEADER = ["news_id", "user_id", "timestamp", "event_type"]
MAX_ROWS = 1_000


if __name__ == '__main__':
    with open("test.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)

        for i in range(MAX_ROWS):

            news_id = random.randint(0, 1000)
            user_id = random.randint(0, 1000)
            timestamp = random.randint(1_000_000, 10_000_000)
            event_type = random.randint(1, 3)

            writer.writerow([news_id, user_id, timestamp, type])
