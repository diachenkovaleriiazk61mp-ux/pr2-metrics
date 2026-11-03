import asyncio
import os
from pathlib import Path

async def app(scope,receive,send):
    if scope['type']=='lifespan':
        while True:
            event=await receive()
            if event['type']=='lifespan.startup':await send({'type':'lifespan.startup.complete'})
            elif event['type']=='lifespan.shutdown':
                Path(os.environ['TEST_CLOSED']).write_text('closed')
                await send({'type':'lifespan.shutdown.complete'});return
    elif scope['type']=='http':
        if scope['path']=='/slow':
            Path(os.environ['TEST_STARTED']).write_text('started')
            await asyncio.sleep(.4)
        await send({'type':'http.response.start','status':200,'headers':[]})
        await send({'type':'http.response.body','body':b'ok'})
