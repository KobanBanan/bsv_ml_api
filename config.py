import asyncio
import aiomysql


async def test_example(loop):
    pool = await aiomysql.create_pool(host='10.168.4.148',
                                      user='fronzilla', password='GP8_4z8%8r++',
                                      db='ML', loop=loop)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 42;")
            print(cur.description)
            (r,) = await cur.fetchone()
            assert r == 42
    pool.close()
    await pool.wait_closed()


loop = asyncio.get_event_loop()
loop.run_until_complete(test_example(loop))