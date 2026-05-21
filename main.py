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

async def consumer(r: redis.Redis):
    last_id = "0-0"

    while True:
        response = await r.xread(
            streams={STREAM_KEY: last_id},
            count=1,
            block=5000,
        )

        if not response:
            print("No new messages")
            continue

        for stream_name, messages in response:
            for message_id, fields in messages:
                print(f"Consumed: {message_id} -> {fields}")
                last_id = message_id

async def main():
    r = redis.Redis()

    await r.delete(STREAM_KEY)

    producer_task = asyncio.create_task(producer(r))
    consumer_task = asyncio.create_task(consumer(r))

    await producer_task

    await asyncio.sleep(3)

    consumer_task.cancel()

    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
