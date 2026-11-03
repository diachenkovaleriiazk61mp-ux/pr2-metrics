import asyncio
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from parallel_loadgen import run_parallel,wait_until

class Noop(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def do_GET(self):
        self.send_response(204);self.send_header('X-Pod','fixture');self.end_headers()
    def log_message(self,*args):pass

class ParallelTests(unittest.TestCase):
    def test_spawn_workers_send_every_request_without_early_timestamps(self):
        server=ThreadingHTTPServer(('127.0.0.1',0),Noop)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            result=run_parallel(f'http://127.0.0.1:{server.server_port}/noop',processes=2,
                                fixed_rate=40,fixed_seconds=1)
            self.assertEqual(len(result['rows']),40)
            self.assertEqual({r['worker'] for r in result['rows']},{0,1})
            self.assertTrue(all(r['status']==204 for r in result['rows']))
            self.assertTrue(all(r['sent']>=r['planned'] and r['latency_ms']>=0 for r in result['rows']))
            self.assertEqual(result['generator_processes'],2)
        finally:server.shutdown();server.server_close();thread.join(2)

    def test_step_transitions_keep_the_shared_schedule(self):
        server=ThreadingHTTPServer(('127.0.0.1',0),Noop)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            result=run_parallel(f'http://127.0.0.1:{server.server_port}/noop',processes=2,
                                warmup=.2,step_seconds=6,first_rate=40,increment=40,safety_max=80)
            self.assertEqual([step['target_rps'] for step in result['steps']],[40,80])
            self.assertEqual(len(result['rows']),8+240+480)
            self.assertEqual(result['stop_reason'],'safety_cap_increase_and_repeat')
            self.assertTrue(all(row['sent']>=row['planned'] for row in result['rows']))
            self.assertEqual(sum(r['status']==204 for r in result['rows']),728)
        finally:server.shutdown();server.server_close();thread.join(2)

class TimerTests(unittest.IsolatedAsyncioTestCase):
    async def test_early_timer_wakeup_is_checked_again(self):
        with patch('parallel_loadgen.time.perf_counter',side_effect=[0,.5,1]),patch('parallel_loadgen.asyncio.sleep',new_callable=AsyncMock) as sleep:
            await wait_until(1)
            self.assertEqual(sleep.await_count,2)
