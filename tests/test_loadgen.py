import sys
import unittest
from pathlib import Path
from aiohttp import web
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from loadgen import run_profile

class GeneratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_http_requests_keep_planned_latency_and_pod(self):
        app=web.Application()
        async def noop(request):return web.Response(status=204,headers={'X-Pod':'fixture'})
        app.router.add_get('/noop',noop)
        runner=web.AppRunner(app);await runner.setup()
        site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
        port=site._server.sockets[0].getsockname()[1]
        try:
            data=await run_profile(f'http://127.0.0.1:{port}/noop',fixed_rate=20,fixed_seconds=.5)
            self.assertEqual(len(data['rows']),10)
            self.assertTrue(all(r['status']==204 and r['pod']=='fixture' for r in data['rows']))
            for r in data['rows']:self.assertGreaterEqual(r['latency_ms'],r['service_roundtrip_ms'])
            self.assertTrue(data['inflight_samples'])
        finally:await runner.cleanup()
