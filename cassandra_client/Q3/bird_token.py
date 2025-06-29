import random
from cassandra.metadata import murmur3

def get_random_bird_id():
    with open("bird_ids.txt", "r") as f:
        bird_ids = [line.strip() for line in f if line.strip()]
    return random.choice(bird_ids)

bird_id = get_random_bird_id().encode()
token = murmur3(get_random_bird_id().encode())
print(f'Bird ID: {bird_id}')
print(f'Token: {token}')