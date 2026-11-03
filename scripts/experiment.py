import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, date
from pathlib import Path
import psutil
from analysis import histogram_quantile, select_capacity, fit_usl
from resources import ProcessTreeSampler

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'results'
SELECTOR='path="/vaccinations/{id}",method="GET"'


def command(*args):
    result=subprocess.run(args,cwd=ROOT,text=True,encoding='utf-8',errors='replace',capture_output=True)
    if result.returncode:
        # Command arguments contain no credentials. Never print Compose's resolved environment.
        raise RuntimeError(' '.join(args)+'\n'+result.stderr[-6000:])
    return result.stdout+result.stderr if args[:2]==('docker','logs') else result.stdout


def compose(*args):return command('docker','compose',*args)

def save(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')


def api(url,method='GET',body=None):
    req=urllib.request.Request(url,method=method,data=json.dumps(body).encode() if body is not None else None,
                               headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=10) as response:
        raw=response.read()
        return json.loads(raw) if raw else None


def configure():
    path=ROOT/'.env'
    if not path.exists():
        path.write_text('POSTGRES_PASSWORD='+secrets.token_hex(24)+'\nPORT=18081\nPROMETHEUS_PORT=19090\nWEB_CPUS=1.5\n')
    settings={}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            key,value=line.split('=',1);settings[key.strip()]=value.strip()
    return 'http://127.0.0.1:'+settings.get('PORT','18081'), 'http://127.0.0.1:'+settings.get('PROMETHEUS_PORT','19090')


def wait_ready(base):
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        try:
            if api(base+'/healthz')['status']=='ready':return
        except (OSError,ValueError):pass
        time.sleep(.3)
    raise TimeoutError('API did not become ready; run docker compose logs web')


def active_targets(prom,count):
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        try:
            targets=api(prom+'/api/v1/targets')['data']['activeTargets']
            web=[t for t in targets if t['labels'].get('job')=='web']
            if len(web)==count and all(t['health']=='up' for t in web):return web
        except (OSError,KeyError,ValueError):pass
        time.sleep(2)
    raise RuntimeError(f'Expected {count} UP web targets; inspect Prometheus /targets')


def scale(base,prom,count):
    compose('up','-d','--wait','--scale',f'web={count}')
    compose('restart','lb')
    wait_ready(base)
    targets=active_targets(prom,count)
    # Ensure at least three fresh scrapes before the first measured window.
    time.sleep(15)
    return targets


def prom_query(prom,query,at=None,start=None,end=None,step=5):
    params={'query':query}
    if start is not None:
        params.update(start=start,end=end,step=step);endpoint='query_range'
    else:
        if at is not None:params['time']=at
        endpoint='query'
    data=api(prom+'/api/v1/'+endpoint+'?'+urllib.parse.urlencode(params))
    if data.get('status')!='success':raise RuntimeError('Prometheus query failed')
    return data['data']['result']


def read_monitor(stop,event_path,generator_pid):
    processes=ProcessTreeSampler(generator_pid)
    with event_path.open('w',encoding='utf-8') as out:
        while not stop.is_set():
            record=dict(timestamp=time.time(),host_cpu_percent=psutil.cpu_percent(None))
            record['generator']=processes.sample()
            try:
                raw=compose('stats','--no-stream','--format','{{json .}}')
                record['containers']=[json.loads(line) for line in raw.splitlines() if line.strip()]
            except Exception as exc:
                record['monitor_error']=str(exc)
            out.write(json.dumps(record,ensure_ascii=False)+'\n');out.flush()
            stop.wait(2)


def load_run(base,path,ceiling=None,safety_max=20000,fixed_rate=None,fixed_seconds=30,on_start=None):
    args=[sys.executable,str(ROOT/'scripts/loadgen.py'),base,'--output',str(path),'--safety-max',str(safety_max)]
    args+=['--processes',os.environ.get('LAB_GENERATOR_PROCESSES','2')]
    if ceiling is not None:args+=['--ceiling',str(ceiling)]
    if fixed_rate is not None:args+=['--fixed-rate',str(fixed_rate),'--fixed-seconds',str(fixed_seconds)]
    with path.with_suffix('.console.log').open('w',encoding='utf-8') as log:
        child=subprocess.Popen(args,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
        stop=threading.Event()
        monitor=threading.Thread(target=read_monitor,args=(stop,path.with_suffix('.resources.jsonl'),child.pid))
        monitor.start()
        action=None
        try:
            if on_start:
                action=threading.Thread(target=on_start);action.start()
            offset=0
            while child.poll() is None:
                time.sleep(.5)
                with path.with_suffix('.console.log').open('r',encoding='utf-8',errors='replace') as progress:
                    progress.seek(offset)
                    message=progress.read()
                    offset=progress.tell()
                if message:print(message,end='',flush=True)
            code=child.returncode
            if code:raise RuntimeError(f'Load generator failed; see {path.with_suffix(".console.log").name}')
        finally:
            stop.set();monitor.join()
            if action:action.join()
    return json.loads(path.read_text(encoding='utf-8'))


def collect_prometheus(prom,run):
    start,end=run['start_epoch'],run['end_epoch']
    expressions={
        'server_p99':'histogram_quantile(0.99,sum by(le)(rate(http_request_duration_seconds_bucket{'+SELECTOR+'}[15s])))',
        'requests_by_instance':'sum by(instance)(rate(http_requests_total{'+SELECTOR+',code=~"2.."}[15s]))',
        'cpu_by_instance':'rate(process_cpu_seconds_total{job="web"}[15s])',
        'inflight_by_instance':'http_requests_in_progress{job="web",path="/vaccinations/{id}"}',
        'throttling_by_instance':'rate(cgroup_cpu_nr_throttled_total{job="web"}[15s])/rate(cgroup_cpu_nr_periods_total{job="web"}[15s])',
        'identity':'service_info{job="web"}',
        'cgroup_available':'cgroup_cpu_stat_available{job="web"}',
    }
    ranges={name:prom_query(prom,expression,start=start,end=end) for name,expression in expressions.items()}
    steps=[]
    for step in run['steps']:
        at=start+step['end']
        values=prom_query(prom,'sum by(le)(increase(http_request_duration_seconds_bucket{'+SELECTOR+'}[15s]))',at=at)
        buckets=sorted((float(v['metric']['le']),float(v['value'][1])) for v in values)
        quantile=histogram_quantile(buckets)
        # Infinity is represented by its text label in raw evidence, not a JSON NaN/Infinity value.
        steps.append(dict(rate=step['target_rps'],time=at,p99=quantile,
                          buckets=[dict(le=str(b),count=c) for b,c in buckets],
                          cpu=prom_query(prom,'rate(process_cpu_seconds_total{job="web"}[15s])',at=at),
                          inflight=prom_query(prom,'avg_over_time(http_requests_in_progress{job="web",path="/vaccinations/{id}"}[15s])',at=at),
                          throttling=prom_query(prom,expressions['throttling_by_instance'],at=at)))
    return dict(queries=expressions,ranges=ranges,steps=steps)


def graceful_check(base,prom,url):
    scale(base,prom,2)
    container=compose('ps','-q','web').splitlines()[0]
    event={}
    def stop_one():
        time.sleep(10)
        event['stop_started_epoch']=time.time()
        try:
            command('docker','stop','--time','35',container)
            event['exit_code']=int(command('docker','inspect','--format','{{.State.ExitCode}}',container).strip())
            event['stop_finished_epoch']=time.time()
            event['logs']=command('docker','logs',container)
        except Exception as exc:event['error']=str(exc)
    run=load_run(url,OUT/'graceful.json',fixed_rate=200,fixed_seconds=30,on_start=stop_one)
    at=event.get('stop_started_epoch',0)-run['start_epoch']
    relevant=[r for r in run['rows'] if at-5 <= r['done'] < at+5 or at-5 <= r['sent'] < at+5]
    errors=[r for r in relevant if not 200<=r['status']<300]
    event.update(window_seconds=[at-5,at+5],requests_in_window=len(relevant),failures=len(errors),
                 passed=event.get('exit_code')==0 and bool(relevant) and not errors and 'Database pool closed' in event.get('logs',''))
    save(OUT/'graceful-check.json',event)
    if not event['passed']:
        raise RuntimeError('Graceful-stop check failed; inspect results/graceful-check.json')


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--processes',type=int,default=2,help='Asynchronous load generator worker processes')
    p.add_argument('--safety-max-rate',type=int,default=20000,help='Calibration safety cap; reaching it does not establish G')
    a=p.parse_args()
    if a.processes<1:p.error('--processes must be positive')
    os.environ['LAB_GENERATOR_PROCESSES']=str(a.processes)
    print('Checking Docker and preparing the lab...',flush=True)
    base,prom=configure()
    machine=json.loads(command('docker','info','--format','{{json .}}'))
    OUT.mkdir(exist_ok=True)
    if any(OUT.iterdir()):
        archive=ROOT/'history'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        archive.mkdir(parents=True)
        for path in list(OUT.iterdir()):shutil.move(str(path),archive/path.name)
    save(OUT/'machine.json',dict(docker_cpus=machine['NCPU'],docker_memory_bytes=machine['MemTotal'],
                                 docker_os=machine.get('OperatingSystem'),host_cpus=psutil.cpu_count(),
                                 host_memory_bytes=psutil.virtual_memory().total,same_host=True,generator_processes=a.processes,
                                 compose_version=compose('version'),base_commit='775a178842a4469d2fd772e1a371d7e9acc25aa9'))
    save(OUT/'status.json',dict(state='running'))
    os.environ['WEB_CPUS']='1.5'
    print('Building and starting the Docker services; first build may take several minutes...',flush=True)
    compose('up','-d','--build','--wait','--scale','web=1');compose('restart','lb');wait_ready(base)
    checks=[]
    api(base+'/livez');api(base+'/healthz')
    ids=[]
    for i in range(100):
        body=dict(patient_code=f'LAB2-{i:03}',vaccine='MMR',batch='L2',dose_number=1,
                  administration_date=date.today().isoformat(),facility='Clinic 7')
        created=api(base+'/vaccinations','POST',body);ids.append(created['id'])
    save(OUT/'seed.json',dict(ids=ids,count=len(ids)))
    url=base+'/vaccinations/'+str(ids[0])
    print('Calibration against /noop',flush=True)
    calibration=load_run(base+'/noop',OUT/'noop.json',safety_max=a.safety_max_rate)
    if calibration['stop_reason']=='safety_cap_increase_and_repeat':
        raise RuntimeError('Calibration reached safety cap before saturation; increase --safety-max-rate and repeat')
    g=max(s['achieved_rps'] for s in calibration['steps'])
    if g<=0:raise RuntimeError('No successful calibration responses')
    save(OUT/'calibration.json',dict(g=g,stop_reason=calibration['stop_reason']))
    selected=None
    for cpu in (1.5,.75):
        os.environ['WEB_CPUS']=str(cpu)
        folder=OUT/('cpu-'+str(cpu));folder.mkdir()
        capacities=[]
        for n in (1,2,3,4):
            print(f'CPU={cpu}, replicas={n}',flush=True)
            targets=scale(base,prom,n);save(folder/f'targets-{n}.json',targets)
            run=load_run(url,folder/f'run-{n}.json',ceiling=g,safety_max=a.safety_max_rate)
            invalid=[s for s in run['steps'] if s['sent_rps']<.98*s['target_rps'] or (s['p95_dispatch_lag_ms'] or 0)>100]
            if invalid:
                save(folder/f'invalid-generator-{n}.json',invalid)
                raise RuntimeError('Generator dispatch fell behind during a service run; raw data saved. Close background load or use a faster host.')
            time.sleep(6)  # Last scrape becomes queryable before exporting the historical ranges.
            metrics=collect_prometheus(prom,run);save(folder/f'prometheus-{n}.json',metrics)
            availability=metrics['ranges']['cgroup_available']
            if not availability or any(float(v)!=1 for series in availability for _,v in series['values']):
                raise RuntimeError('CPU cgroup counters unavailable on a web target; M3 cannot be measured.')
            capacity=select_capacity(run,g,n)
            # Reaching the generator ceiling means the offered workload did not establish a service limit.
            save(folder/f'capacity-{n}.json',capacity);capacities.append(capacity)
            since=datetime.fromtimestamp(run['start_epoch'],timezone.utc).isoformat()
            (folder/f'lb-{n}.log').write_text(compose('logs','--no-color','--since',since,'lb'),encoding='utf-8')
        fit=fit_usl(capacities)
        save(folder/'usl.json',fit)
        if fit['valid']:
            selected=folder;break
        print('Not enough service-limited points; CPU will be halved if this was the first series.',flush=True)
    if selected is None:
        save(OUT/'status.json',dict(state='incomplete',reason='Generator-limited points remain after halving CPU'))
        raise RuntimeError('Cannot fit USL after CPU reduction; evidence saved. Use a faster/separate generator host.')
    save(OUT/'selected-series.json',dict(folder=selected.name,web_cpus=float(os.environ['WEB_CPUS']),pool_size=15,g=g))
    command(sys.executable,str(ROOT/'scripts/analyze.py'))
    # All Prometheus series and charts have been exported before any manual cleanup.
    graceful_check(base,prom,url)
    save(OUT/'status.json',dict(state='complete',selected_series=selected.name))
    print('Measurements complete. Open RESULTS.md, figures/ and results/graceful-check.json.',flush=True)
    print('REPORT.md is a separate narrative report; complete its marked measurement sections using RESULTS.md.',flush=True)

if __name__=='__main__':
    try:
        main()
    except Exception as exc:
        if OUT.exists():save(OUT/'status.json',dict(state='incomplete',reason=str(exc)))
        print(str(exc),file=sys.stderr)
        sys.exit(1)
