import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from resources import ProcessTreeSampler

class FakeProcess:
    def __init__(self,pid,cpu,rss,children=()):self.pid,self.cpu,self.rss,self.descendants=pid,cpu,rss,children
    def cpu_percent(self,interval):return self.cpu
    def memory_info(self):return SimpleNamespace(rss=self.rss)
    def children(self,recursive):return self.descendants

class ResourcesTests(unittest.TestCase):
    def test_windows_launcher_and_worker_processes_are_summed(self):
        worker=FakeProcess(3,60,100)
        interpreter=FakeProcess(2,10,80)
        launcher=FakeProcess(1,0,1,[interpreter,worker])
        with patch('resources.psutil.Process',return_value=launcher):
            sampler=ProcessTreeSampler(1)
            first=sampler.sample();second=sampler.sample()
        self.assertEqual(first['cpu_percent'],0)
        self.assertEqual(second['cpu_percent'],70)
        self.assertEqual(second['rss_bytes'],181)
        self.assertTrue(second['includes_descendants'])
