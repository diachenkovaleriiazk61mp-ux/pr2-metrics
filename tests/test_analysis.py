import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from analysis import fit_usl,histogram_quantile,select_capacity
from loadgen import step_summary
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from metrics import CgroupCPU

class AnalysisTests(unittest.TestCase):
    def test_fit_recovers_known_parameters(self):
        points=[dict(replicas=n,x=100*n/(1+.12*(n-1)+.025*n*(n-1)),generator_limited=False) for n in (1,2,3,4)]
        fit=fit_usl(points)
        self.assertAlmostEqual(fit['sigma'],.12)
        self.assertAlmostEqual(fit['kappa'],.025)
        self.assertAlmostEqual(fit['n_star'],math.sqrt(.88/.025))
    def test_excludes_generator_limited_and_nonnegative_fit(self):
        points=[dict(replicas=n,x=100*n/(1+.2*(n-1)),generator_limited=(n==4)) for n in (1,2,3,4)]
        fit=fit_usl(points);self.assertTrue(fit['valid']);self.assertGreaterEqual(fit['kappa'],0)
        points[2]['generator_limited']=True
        self.assertFalse(fit_usl(points)['valid'])
    def test_histogram_interpolation_and_overflow(self):
        q=histogram_quantile([(.005,80),(.01,100),(float('inf'),100)])
        self.assertAlmostEqual(q['value'],.00975)
        self.assertEqual(q['error_bound'],.005)
        q=histogram_quantile([(1,90),(float('inf'),100)])
        self.assertTrue(q['overflow']);self.assertIsNone(q['error_bound'])
    def test_timeouts_non2xx_and_last_fifteen_seconds(self):
        rows=[]
        for sent,done,status,kind in [(1,1.1,200,None),(6,6.1,200,None),(7,12,0,'timeout'),(8,8.1,0,'connection'),(9,9.1,503,None)]:
            rows.append(dict(sent=sent,done=done,status=status,error_kind=kind,pod='a',latency_ms=(done-sent)*1000,lag_ms=0))
        step=step_summary(rows,0,100)
        self.assertEqual(step['success_count'],1);self.assertEqual(step['non2xx_count'],2)
        self.assertEqual(step['connection_errors'],1);self.assertEqual(step['achieved_rps'],1/15)
    def test_dispatch_counts_unfinished_requests(self):
        dispatch=[dict(sent=6,lag_ms=0),dict(sent=7,lag_ms=1)]
        step=step_summary([],0,100,sent_records=dispatch)
        self.assertEqual(step['sent_rps'],2/15)
        self.assertEqual(step['success_count'],0)

    def test_cgroup_v2_and_v1_counters(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'cpu.stat';path.write_text('nr_periods 100\nnr_throttled 25\nusage_usec 7\n')
            samples=[s for metric in CgroupCPU([Path(folder)/'missing',path]).collect() for s in metric.samples]
            values={s.name:s.value for s in samples}
            self.assertEqual(values['cgroup_cpu_nr_periods_total'],100)
            self.assertEqual(values['cgroup_cpu_nr_throttled_total'],25)
