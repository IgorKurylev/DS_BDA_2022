import random
import csv
import time
from datetime import datetime

HEADER = ["x", "y", "timestamp_millis", "datetime"]


if __name__ == "__main__":

    f = open("mouse_events_random3.csv", "w")
    writer = csv.writer(f)
    writer.writerow(HEADER)

    i = 0

    while i != 160_000_000:
        if i % 500_000 == 0:
            print(f"i = {i}")

        x = random.randint(0, 1920)
        y = random.randint(0, 1080)
        writer.writerow([x, y, int(round(time.time() * 1000)), datetime.now()])
        i += 1
