import asyncio
import redis.asyncio as redis

STREAM_KEY = "events"

async def producer(r: redis.Redis):
    for i in range(50):
        event_id = await r.xadd(STREAM_KEY, {
            "event_type": "test.event",
            "number": str(i),
        })

        print(f"Produced: {event_id}")
        await asyncio.sleep(1)
