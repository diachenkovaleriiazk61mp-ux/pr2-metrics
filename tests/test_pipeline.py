import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import analyze

class PipelineTests(unittest.TestCase):
    def test_tables_and_all_figures_from_fixture_data(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);out=root/'results';out.mkdir();series=out/'cpu-1.5';series.mkdir();fig=root/'figures'
            def save(path,data):path.write_text(json.dumps(data))
            save(out/'selected-series.json',dict(folder=series.name,g=5000,web_cpus=1.5))
            save(out/'machine.json',dict(docker_cpus=8,docker_memory_bytes=8*2**30))
            for n in (1,2,3,4):
                x=200*n/(1+.1*(n-1)+.02*n*(n-1))
                best=dict(start=10,end=30,window_start=15,window_end=30,target_rps=1000,achieved_rps=x,
                          std_rps=3,sent_rps=1000,p99_ms=5,non2xx_fraction=.01,connection_errors=1)
                step2=dict(best,start=30,end=50,window_start=35,window_end=50,target_rps=1200,achieved_rps=x*.9)
                seconds=[dict(second=i,target_rps=1000,achieved_rps=x,p99_ms=5,sent_rps=1000) for i in range(50)]
                save(series/f'run-{n}.json',dict(start_epoch=1000,end_epoch=1050,profile_end=50,steps=[best,step2],
                    seconds=seconds,rows=[dict(sent=16,done=16.1,status=200,pod=f'pod-{j}') for j in range(n)]))
                vector=[dict(metric={'instance':f'ip-{j}'},value=[1030,'0.2']) for j in range(n)]
                ranges={key:[dict(metric={'instance':f'ip-{j}'},values=[[1015,'0.1'],[1020,'0.2']]) for j in range(n)]
                        for key in ('server_p99','requests_by_instance','cpu_by_instance','inflight_by_instance','throttling_by_instance')}
                promstep=dict(p99=dict(value=.008,lower=.005,upper=.01,error_bound=.005,overflow=False),
                              cpu=vector,inflight=vector,throttling=vector)
                save(series/f'prometheus-{n}.json',dict(ranges=ranges,steps=[dict(promstep,rate=1000),dict(promstep,rate=1200)]))
            (series/'run-4.resources.jsonl').write_text(json.dumps(dict(timestamp=1022,host_cpu_percent=50,
                generator=dict(cpu_percent=60,rss_bytes=2**24),containers=[dict(Name='db',CPUPerc='10%',MemUsage='10MiB / 1GiB')]))+'\n')
            with patch.object(analyze,'ROOT',root),patch.object(analyze,'OUT',out),patch.object(analyze,'FIG',fig):analyze.main()
            self.assertTrue((root/'RESULTS.md').exists())
            self.assertEqual(len(list(fig.glob('*.png'))),6)
            self.assertIn('σ=',(root/'RESULTS.md').read_text())
