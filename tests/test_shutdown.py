import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

@unittest.skipIf(os.name=='nt','SIGTERM process integration test runs on Linux/Docker')
class ShutdownTests(unittest.TestCase):
    def test_sigterm_drains_request_and_exits_zero(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
        with tempfile.TemporaryDirectory() as folder:
            started=Path(folder)/'started';closed=Path(folder)/'closed'
            env=dict(os.environ,TEST_STARTED=str(started),TEST_CLOSED=str(closed))
            source=f"from server import LabServer; import uvicorn; LabServer(uvicorn.Config('tests.slow_app:app',host='127.0.0.1',port={port},access_log=False)).run()"
            process=subprocess.Popen([sys.executable,'-c',source],cwd=ROOT,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            try:
                for _ in range(100):
                    try:
                        with urllib.request.urlopen(f'http://127.0.0.1:{port}/livez',timeout=.2):break
                    except OSError:time.sleep(.05)
                result=[]
                def request():
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/slow',timeout=5) as response:result.append(response.status)
                thread=threading.Thread(target=request);thread.start()
                for _ in range(100):
                    if started.exists():break
                    time.sleep(.01)
                self.assertTrue(started.exists())
                process.send_signal(signal.SIGTERM)
                thread.join(5)
                self.assertEqual(result,[200]);self.assertEqual(process.wait(timeout=5),0)
                self.assertTrue(closed.exists())
            finally:
                if process.poll() is None:process.kill();process.wait()
