import asyncio

from redis import ResponseError
import redis.asyncio as redis

STREAM_KEY = "events"
CONSUMER_GROUP = "analytics-ingestors"
NUM_WORKERS = 10

# creates a consumer group
async def create_group(r: redis.Redis):
    try:
        await r.xgroup_create(STREAM_KEY, groupname=CONSUMER_GROUP, id="0", mkstream=True)
    except ResponseError as e:
        print(f"raised: {e}")

async def consumer_manager(r: redis.Redis):
    tasks = []

    try:
        # create a task for each worker
        for i in range(NUM_WORKERS):
            worker_name = f"worker-{i}"

            task = asyncio.create_task(worker(r, worker_name=worker_name))
            tasks.append(task)
        
        await asyncio.gather(*tasks)

    # if manager task gets cancelled, cancel all inner-tasks as well
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        
        await asyncio.gather(*tasks, return_exceptions=True)

        raise

async def worker(r: redis.Redis, worker_name):
    while True:
        response = await r.xreadgroup(CONSUMER_GROUP, worker_name, block=5000, count=2, streams={STREAM_KEY: ">"})

        if not response:
            print("No new messages")
            continue
        
        print("Response: ", response)

        for stream_name, messages in response:
            for message_id, fields in messages:
                print(f"Consumed: {message_id} -> {fields}")
                await r.xack(STREAM_KEY, CONSUMER_GROUP, message_id)
