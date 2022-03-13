from pynput.mouse import Listener
from datetime import datetime
import csv
import time


HEADER = ["x", "y", "timestamp_millis", "datetime"]

f = open("mouse_events.csv", "w")
writer = csv.writer(f)
writer.writerow(HEADER)


def on_move(x, y):
    writer.writerow([x, y, int(round(time.time() * 1000)), datetime.now()])


def on_click(x, y, button, pressed):
    if pressed:
        writer.writerow([x, y, int(round(time.time() * 1000)), datetime.now()])


if __name__ == "__main__":
    with Listener(on_move=on_move, on_click=on_click) as listener:
        listener.join()
