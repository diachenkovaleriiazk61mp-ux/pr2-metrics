import asyncio
import os
import sys
import unittest
from contextlib import asynccontextmanager
from datetime import date,timedelta
from pathlib import Path
from unittest.mock import patch
import httpx
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app import create_app
from validation import validate

class Cursor:
    def __init__(self,rows):self.rows=rows
    async def fetchone(self):return self.rows[0] if self.rows else None
    async def fetchall(self):return self.rows

class Pool:
    def __init__(self):self.rows={};self.next=1;self.offline=False
    @asynccontextmanager
    async def connection(self):
        if self.offline:
            from psycopg import OperationalError
            raise OperationalError('test offline')
        yield self
    async def execute(self,sql,args=()):
        from validation import FIELDS
        if sql.startswith('INSERT'):
            row=dict(zip(FIELDS,args),id=self.next);self.next+=1;self.rows[row['id']]=row
            return Cursor([row])
        if sql.startswith('UPDATE'):
            ident=args[-1]
            if ident not in self.rows:return Cursor([])
            self.rows[ident]=dict(zip(FIELDS,args[:-1]),id=ident);return Cursor([self.rows[ident]])
        if sql.startswith('DELETE'):
            row=self.rows.pop(args[0],None);return Cursor([row] if row else [])
        if sql.startswith('SELECT count'):
            rows=[r for r in self.rows.values() if not args or r['vaccine']==args[0]]
            return Cursor([dict(total=len(rows))])
        if 'WHERE id=' in sql:
            row=self.rows.get(args[0]);return Cursor([row] if row else [])
        if sql.startswith('SELECT *'):
            rows=[r for r in self.rows.values() if len(args)==2 or r['vaccine']==args[0]]
            return Cursor(rows[args[-1]:args[-1]+args[-2]])
        return Cursor([dict(ok=1)])

class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pool=Pool()
        with patch.dict(os.environ,{'POD_NAME':'test-pod','DB_POOL_SIZE':'15'}):self.app=create_app(self.pool)
        self.client=httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app),base_url='http://test')
        self.good=dict(patient_code='P7',vaccine='MMR',batch='B7',dose_number=1,
                       administration_date=date.today().isoformat(),facility='Clinic')
    async def asyncTearDown(self):await self.client.aclose()
    async def test_crud_and_metrics_use_path_template(self):
        response=await self.client.post('/vaccinations',json=self.good)
        self.assertEqual(response.status_code,201);ident=response.json()['id']
        self.assertEqual(response.headers['X-Pod'],'test-pod')
        self.assertEqual((await self.client.get('/vaccinations?vaccine=MMR')).json()['total'],1)
        self.assertEqual((await self.client.get(f'/vaccinations/{ident}')).status_code,200)
        self.assertEqual((await self.client.put(f'/vaccinations/{ident}',json=dict(self.good,dose_number=2))).json()['dose_number'],2)
        self.assertEqual((await self.client.delete(f'/vaccinations/{ident}')).status_code,204)
        self.assertEqual((await self.client.delete(f'/vaccinations/{ident}')).status_code,404)
        text=(await self.client.get('/metrics')).text
        self.assertIn('path="/vaccinations/{id}"',text)
        self.assertNotIn('path="/vaccinations/1"',text)
        self.assertIn('http_request_duration_seconds_bucket',text)
        self.assertIn('le="0.005"',text)
        self.assertEqual(self.app.metrics.inflight.labels('/vaccinations/{id}')._value.get(),0)
    async def test_livez_does_not_use_database(self):
        self.pool.offline=True
        self.assertEqual((await self.client.get('/livez')).status_code,200)
        self.assertEqual((await self.client.get('/healthz')).status_code,503)
        self.assertEqual((await self.client.get('/vaccinations/1')).status_code,503)
    async def test_validation_including_control_characters(self):
        for changes in (dict(dose_number=5),dict(dose_number=True),dict(patient_code='P\x00Q'),
                        dict(facility='bad\nvalue'),dict(administration_date=(date.today()+timedelta(days=1)).isoformat())):
            response=await self.client.post('/vaccinations',json=dict(self.good,**changes))
            self.assertEqual(response.status_code,400,changes)
        self.assertEqual((await self.client.put('/vaccinations/1',json={'vaccine':'MMR'})).status_code,400)
        self.assertEqual((await self.client.get('/vaccinations?limit=101')).status_code,400)
        self.assertEqual((await self.client.post('/vaccinations',content=b'{',headers={'Content-Type':'application/json'})).status_code,400)
        self.assertEqual((await self.client.post('/vaccinations',content=b' '*17000,headers={'Content-Type':'application/json'})).status_code,400)

class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_queue_returns_503_but_metrics_remain_available(self):
        self.entered=asyncio.Event();self.release=asyncio.Event()
        class SlowPool(Pool):
            async def execute(inner,sql,args=()):
                if 'WHERE id=' in sql:
                    self.entered.set();await self.release.wait()
                return await super().execute(sql,args)
        with patch.dict(os.environ,{'MAX_INFLIGHT_REQUESTS':'1'}):app=create_app(SlowPool())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            first=asyncio.create_task(client.get('/vaccinations/1'))
            await self.entered.wait()
            rejected=await client.get('/vaccinations/2')
            self.assertEqual(rejected.status_code,503)
            self.assertIn('capacity',rejected.json()['errors'])
            self.assertEqual((await client.get('/metrics')).status_code,200)
            self.assertEqual(app.workload_active,1)
            self.release.set();await first
            self.assertEqual(app.workload_active,0)
