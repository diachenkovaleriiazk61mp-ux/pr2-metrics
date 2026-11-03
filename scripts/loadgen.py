import argparse
import asyncio
import json
import math
import statistics
import time
from collections import Counter
from pathlib import Path
import aiohttp


def percentile(values, q):
    if not values:
        return None
    values = sorted(values)
    p = (len(values)-1)*q
    lo = int(p)
    return values[lo]+(values[min(lo+1,len(values)-1)]-values[lo])*(p-lo)


def step_summary(rows, start, rate, duration=20, sent_records=None):
    lo, hi = start+5, start+duration
    completions = [r for r in rows if lo <= r['done'] < hi]
    sent = [r for r in (rows if sent_records is None else sent_records) if lo <= r['sent'] < hi]
    counts = [sum(200 <= r['status'] < 300 and lo+i <= r['done'] < lo+i+1 for r in completions)
              for i in range(duration-5)]
    good = sum(counts)
    http_bad = sum(r['status'] >= 300 or 0 < r['status'] < 200 for r in completions)
    timeout = sum(r['error_kind']=='timeout' for r in completions)
    connection = sum(r['error_kind']=='connection' for r in completions)
    n = len(completions)
    return dict(start=start, end=start+duration, window_start=lo, window_end=hi, target_rps=rate,
                achieved_rps=good/(hi-lo), std_rps=statistics.stdev(counts) if len(counts)>1 else 0,
                sent_rps=len(sent)/(hi-lo), success_count=good, http_non2xx=http_bad,
                timeouts=timeout, non2xx_count=http_bad+timeout, connection_errors=connection,
                non2xx_fraction=(http_bad+timeout)/n if n else None,
                failure_fraction=(http_bad+timeout+connection)/n if n else None,
                p50_ms=percentile([r['latency_ms'] for r in completions],.5),
                p95_ms=percentile([r['latency_ms'] for r in completions],.95),
                p99_ms=percentile([r['latency_ms'] for r in completions],.99),
                p95_dispatch_lag_ms=percentile([r['lag_ms'] for r in sent],.95),
                pod_counts=dict(Counter(r['pod'] for r in completions if r['pod'])))


def seconds_table(rows, steps, elapsed, inflight_samples):
    size=math.ceil(elapsed)
    sent_bins=[[] for _ in range(size)]
    done_bins=[[] for _ in range(size)]
    active_bins=[[] for _ in range(size)]
    for row in rows:
        if 0<=row['sent']<size:sent_bins[int(row['sent'])].append(row)
        if 0<=row['done']<size:done_bins[int(row['done'])].append(row)
    for sample in inflight_samples:
        if 0<=sample['second']<size:active_bins[int(sample['second'])].append(sample['count'])
    result=[]
    for second in range(size):
        finished=done_bins[second];sent=sent_bins[second]
        step=next((s for s in steps if s['start']<=second<s['end']),None)
        target=step['target_rps'] if step else (200 if second<10 else 0)
        timeout=sum(r['error_kind']=='timeout' for r in finished)
        bad=sum(r['status']>=300 or 0<r['status']<200 for r in finished)
        good=sum(200<=r['status']<300 for r in finished)
        result.append(dict(second=second,target_rps=target,sent_rps=len(sent),achieved_rps=good,
                    responses_2xx=good,responses_non2xx=bad+timeout,timeouts=timeout,
                    connection_errors=sum(r['error_kind']=='connection' for r in finished),
                    p50_ms=percentile([r['latency_ms'] for r in finished],.5),
                    p95_ms=percentile([r['latency_ms'] for r in finished],.95),
                    p99_ms=percentile([r['latency_ms'] for r in finished],.99),
                    inflight=statistics.mean(active_bins[second]) if active_bins[second] else None))
    return result


async def run_profile(url, ceiling=None, safety_max=20000, warmup=10, step_seconds=20,
                      first_rate=200, increment=200, fixed_rate=None, fixed_seconds=30):
    if safety_max < first_rate or step_seconds < 6:
        raise ValueError('Invalid profile configuration')
    rows, pending, steps = [], set(), []
    active=0
    inflight_samples=[]
    sent_events=[]
    sampling=True
    origin = time.perf_counter()+.1
    epoch = time.time()+(origin-time.perf_counter())
    timeout = aiohttp.ClientTimeout(total=5,ceil_threshold=math.inf)
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=0, ttl_dns_cache=5)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async def request(planned):
            nonlocal active
            from parallel_loadgen import wait_until
            await wait_until(planned)
            active+=1
            sent = time.perf_counter()
            sent_events.append(dict(sent=sent-origin,lag_ms=(sent-planned)*1000))
            status, pod, kind, error = 0, '', None, None
            try:
                async with session.get(url, allow_redirects=False) as response:
                    await response.read()
                    status, pod = response.status, response.headers.get('X-Pod','')
            except (asyncio.TimeoutError, TimeoutError) as exc:
                kind, error = 'timeout', type(exc).__name__
            except (aiohttp.ClientError, OSError) as exc:
                kind, error = 'connection', repr(exc)
            done = time.perf_counter()
            active-=1
            rows.append(dict(planned=planned-origin, sent=sent-origin, done=done-origin,
                             status=status, pod=pod, latency_ms=(done-planned)*1000,
                             service_roundtrip_ms=(done-sent)*1000, lag_ms=(sent-planned)*1000,
                             error_kind=kind, error=error))

        async def sample_inflight():
            while sampling:
                inflight_samples.append(dict(second=time.perf_counter()-origin,count=active))
                await asyncio.sleep(.1)

        sampler=asyncio.create_task(sample_inflight())

        async def schedule(rate, start, seconds):
            for index in range(round(rate*seconds)):
                planned = origin+start+index/rate
                delay = planned-time.perf_counter()
                if delay>0:
                    await asyncio.sleep(delay)
                if len(pending)>200000:
                    raise RuntimeError('More than 200000 outstanding requests: generator is overloaded')
                task = asyncio.create_task(request(planned))
                pending.add(task)
                task.add_done_callback(pending.discard)
                if index%64==0:
                    await asyncio.sleep(0)
            delay = origin+start+seconds-time.perf_counter()
            if delay>0:
                await asyncio.sleep(delay)

        if fixed_rate is not None:
            await schedule(fixed_rate,0,fixed_seconds)
            steps=[dict(start=0,end=fixed_seconds,target_rps=fixed_rate)]
            stop_reason='fixed_duration'
            profile_end=fixed_seconds
        else:
            await schedule(first_rate,0,warmup)
            below, start, rate = 0, warmup, first_rate
            finished_cursor=sent_cursor=0
            while True:
                await schedule(rate,start,step_seconds)
                step=step_summary(rows[finished_cursor:],start,rate,step_seconds,sent_events[sent_cursor:])
                finished_cursor=len(rows);sent_cursor=len(sent_events)
                steps.append(step)
                print(json.dumps(dict(target=rate, achieved=step['achieved_rps'],sent=step['sent_rps'])),flush=True)
                below=below+1 if step['achieved_rps']<.9*rate else 0
                if below>=2:
                    stop_reason='two_underloaded_steps'
                    break
                if ceiling is not None and rate>=ceiling:
                    stop_reason='generator_ceiling'
                    break
                if rate>=safety_max:
                    stop_reason='safety_cap_increase_and_repeat'
                    break
                start+=step_seconds
                rate+=increment
            profile_end=steps[-1]['end']
        if pending:
            await asyncio.gather(*list(pending))
        sampling=False
        await sampler
    return dict(inflight_samples=inflight_samples,start_epoch=epoch,end_epoch=epoch+profile_end, profile_end=profile_end,
                timeout_seconds=5,stop_reason=stop_reason,steps=steps,rows=sorted(rows,key=lambda r:r['sent']),
                seconds=seconds_table(rows,steps,profile_end,inflight_samples))


def main():
    p=argparse.ArgumentParser()
    p.add_argument('url');p.add_argument('--ceiling',type=float)
    p.add_argument('--safety-max',type=int,default=20000)
    p.add_argument('--fixed-rate',type=int);p.add_argument('--fixed-seconds',type=int,default=30)
    p.add_argument('--output',required=True)
    p.add_argument('--processes',type=int,default=2)
    a=p.parse_args()
    from parallel_loadgen import run_parallel, windows_timer
    with windows_timer():
        result=run_parallel(a.url,a.ceiling,a.safety_max,processes=a.processes,fixed_rate=a.fixed_rate,fixed_seconds=a.fixed_seconds)
    dest=Path(a.output);dest.parent.mkdir(parents=True,exist_ok=True)
    dest.write_text(json.dumps(result,ensure_ascii=False,separators=(',',':')),encoding='utf-8')

if __name__=='__main__':
    main()
