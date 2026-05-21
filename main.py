import asyncio
import redis.asyncio as redis

from consumer import consumer_manager, create_group
from producer import STREAM_KEY, producer

async def main():
    r = redis.Redis(decode_responses=True)

    await r.delete(STREAM_KEY)

    # create consumer group
    await create_group(r)

    # start producer task
    producer_task = asyncio.create_task(producer(r))

    # start consumer manager task
    consumer_manager_task = asyncio.create_task(consumer_manager(r))

    # await producer
    await producer_task

    await asyncio.sleep(3)

    consumer_manager_task.cancel()

    try:
        await consumer_manager_task
    except asyncio.CancelledError:
        pass

    await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
