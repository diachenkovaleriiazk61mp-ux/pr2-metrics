import asyncio
import json
import math
import multiprocessing as mp
import os
import queue
import time
import traceback
from contextlib import contextmanager
import aiohttp
from loadgen import seconds_table, step_summary


@contextmanager
def windows_timer():
    library=None
    if os.name=='nt':
        import ctypes
        library=ctypes.WinDLL('winmm')
        if library.timeBeginPeriod(1)!=0:
            library=None
    try:
        yield
    finally:
        if library is not None:library.timeEndPeriod(1)


async def wait_until(when):
    # asyncio timers on Windows may wake slightly early. Recheck the actual clock.
    while True:
        remaining=when-time.perf_counter()
        if remaining<=0:return
        await asyncio.sleep(remaining)


async def worker_loop(url,rank,workers,control,output):
    pending=set()
    rows,dispatch,samples=[],[],[]
    active=0
    origin=None
    running=True

    def flush():
        nonlocal rows,dispatch,samples
        if rows or dispatch or samples:
            output.put(dict(kind='batch',rank=rank,rows=rows,sent=dispatch,samples=samples))
            rows,dispatch,samples=[],[],[]

    async def publish():
        while running:
            if origin is not None:
                samples.append(dict(second=time.perf_counter()-origin,count=active,worker=rank))
            flush()
            await asyncio.sleep(.1)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5,ceil_threshold=math.inf),
                                    connector=aiohttp.TCPConnector(limit=0,limit_per_host=0,ttl_dns_cache=5)) as client:
        async def request(planned):
            nonlocal active
            await wait_until(planned)
            sent=time.perf_counter();active+=1
            dispatch.append(dict(sent=sent-origin,lag_ms=(sent-planned)*1000))
            status,pod,kind,error,excerpt=0,'',None,None,None
            try:
                async with client.get(url,allow_redirects=False) as response:
                    body=await response.read()
                    status,pod=response.status,response.headers.get('X-Pod','')
                    if status>=400:excerpt=body[:256].decode('utf-8',errors='replace')
            except (asyncio.TimeoutError,TimeoutError) as exc:
                kind,error='timeout',type(exc).__name__
            except (aiohttp.ClientError,OSError) as exc:
                kind,error='connection',repr(exc)
            done=time.perf_counter();active-=1
            rows.append(dict(planned=planned-origin,sent=sent-origin,done=done-origin,status=status,pod=pod,
                             latency_ms=(done-planned)*1000,service_roundtrip_ms=(done-sent)*1000,
                             lag_ms=(sent-planned)*1000,error_kind=kind,error=error,
                             response_excerpt=excerpt,worker=rank))

        output.put(dict(kind='ready',rank=rank))
        publisher=asyncio.create_task(publish())
        while True:
            plan=await asyncio.to_thread(control.get)
            if plan is None:break
            origin=plan['origin']
            for index in range(rank,round(plan['rate']*plan['duration']),workers):
                planned=origin+plan['start']+index/plan['rate']
                await wait_until(planned)
                if len(pending)>100000:
                    raise RuntimeError('Generator worker has more than 100000 outstanding requests')
                task=asyncio.create_task(request(planned))
                pending.add(task);task.add_done_callback(pending.discard)
                if index%64==rank:await asyncio.sleep(0)
            await wait_until(origin+plan['start']+plan['duration'])
            flush()
            output.put(dict(kind='stage_done',rank=rank,stage=plan['stage']))
        if pending:await asyncio.gather(*list(pending))
        running=False
        await publisher
        flush()
        output.put(dict(kind='finished',rank=rank))


def worker_entry(url,rank,workers,control,output):
    try:
        with windows_timer():asyncio.run(worker_loop(url,rank,workers,control,output))
    except BaseException:
        output.put(dict(kind='error',rank=rank,error=traceback.format_exc()))


def merge_inflight(samples):
    values={}
    for sample in samples:
        tick=math.floor(sample['second']*10)
        values.setdefault(tick,{})[sample['worker']]=sample['count']
    last={};result=[]
    for tick,updates in sorted(values.items()):
        last.update(updates)
        result.append(dict(second=tick/10,count=sum(last.values())))
    return result


def run_parallel(url,ceiling=None,safety_max=20000,processes=2,fixed_rate=None,fixed_seconds=30,
                 warmup=10,step_seconds=20,first_rate=200,increment=200):
    if processes<1 or step_seconds<6 or safety_max<first_rate:
        raise ValueError('Invalid generator configuration')
    ctx=mp.get_context('spawn')
    output=ctx.Queue()
    controls=[ctx.Queue() for _ in range(processes)]
    workers=[ctx.Process(target=worker_entry,args=(url,i,processes,controls[i],output)) for i in range(processes)]
    rows,sent,samples,steps=[],[],[],[]
    for worker in workers:worker.start()

    def receive_until(kind,stage=None):
        received=set()
        while len(received)<processes:
            try:message=output.get(timeout=30)
            except queue.Empty:
                if any(not worker.is_alive() for worker in workers):
                    raise RuntimeError('Generator worker exited unexpectedly')
                raise RuntimeError('No generator progress for 30 seconds')
            if message['kind']=='error':raise RuntimeError(message['error'])
            if message['kind']=='batch':
                rows.extend(message['rows']);sent.extend(message['sent']);samples.extend(message['samples'])
            elif message['kind']==kind and (stage is None or message.get('stage')==stage):
                received.add(message['rank'])

    try:
        receive_until('ready')
        origin=time.perf_counter()+1
        epoch=time.time()+(origin-time.perf_counter())
        def send_stage(number,start,rate,duration):
            for control in controls:control.put(dict(stage=number,start=start,rate=rate,duration=duration,origin=origin))
            receive_until('stage_done',number)
        if fixed_rate is not None:
            send_stage(0,0,fixed_rate,fixed_seconds)
            steps=[dict(start=0,end=fixed_seconds,target_rps=fixed_rate)]
            stop_reason='fixed_duration';profile_end=fixed_seconds
        else:
            send_stage(0,0,first_rate,warmup)
            start,rate,below,stage=warmup,first_rate,0,1
            row_cursor,sent_cursor=len(rows),len(sent)
            while True:
                send_stage(stage,start,rate,step_seconds)
                step=step_summary(rows[row_cursor:],start,rate,step_seconds,sent[sent_cursor:])
                row_cursor,sent_cursor=len(rows),len(sent)
                steps.append(step)
                print(json.dumps(dict(target=rate,achieved=step['achieved_rps'],sent=step['sent_rps'],
                                      dispatch_lag_p95_ms=step['p95_dispatch_lag_ms'])),flush=True)
                below=below+1 if step['achieved_rps']<.9*rate else 0
                if below>=2:stop_reason='two_underloaded_steps';break
                if ceiling is not None and rate>=ceiling:stop_reason='generator_ceiling';break
                if rate>=safety_max:stop_reason='safety_cap_increase_and_repeat';break
                start+=step_seconds;rate+=increment;stage+=1
            profile_end=steps[-1]['end']
        for control in controls:control.put(None)
        receive_until('finished')
        inflight=merge_inflight(samples)
        return dict(generator_processes=processes,inflight_samples=inflight,start_epoch=epoch,
                    end_epoch=epoch+profile_end,profile_end=profile_end,timeout_seconds=5,
                    stop_reason=stop_reason,steps=steps,rows=sorted(rows,key=lambda r:r['sent']),
                    seconds=seconds_table(rows,steps,profile_end,inflight))
    finally:
        for worker in workers:
            worker.join(timeout=2)
            if worker.is_alive():worker.terminate();worker.join(timeout=2)
        for control in controls:control.close()
        output.close()
