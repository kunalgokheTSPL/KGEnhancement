import random
from datetime import datetime

def generate_uid() -> str:
    now = int(datetime.now().timestamp())
    rand = random.randint(1000, 9999)
    unique_number = (now * 10000 + rand) % 10**8
    return int(str(unique_number).zfill(8))