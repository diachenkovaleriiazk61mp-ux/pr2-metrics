import gzip
import hashlib
import json
import os
import shutil
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
EXCLUDED={'.git','.venv','__pycache__','history','submission'}


def split_raw(source,destination,chunk_size=50000):
    data=json.loads(source.read_text(encoding='utf-8'))
    keys=list(data)
    rows=data.pop('rows')
    destination.mkdir(parents=True,exist_ok=True)
    parts=[]
    for start in range(0,len(rows),chunk_size):
        name=f'rows-{start//chunk_size+1:03}.json.gz'
        with gzip.open(destination/name,'wt',encoding='utf-8') as out:
            json.dump(rows[start:start+chunk_size],out,ensure_ascii=False,separators=(',',':'))
        parts.append(name)
    metadata=dict(root_keys=keys,other_fields=data,parts=parts,row_count=len(rows),
                  original_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    (destination/'metadata.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding='utf-8')


def main():
    destination=ROOT/'submission'
    if destination.exists():
        raise SystemExit('submission already exists. Rename or remove that generated folder before packaging again.')
    destination.mkdir()
    for directory,folders,files in os.walk(ROOT):
        folders[:]=[f for f in folders if f not in EXCLUDED]
        for name in files:
            path=Path(directory)/name
            relative=path.relative_to(ROOT)
            if name=='.env' or path.suffix in ('.pyc','.zip'):continue
            if relative.as_posix()=='results/noop.json' and path.stat().st_size>20*2**20:
                split_raw(path,destination/'results/noop-raw')
            else:
                target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(path,target)
    large=[p.relative_to(destination).as_posix() for p in destination.rglob('*') if p.is_file() and p.stat().st_size>25*2**20]
    if large:raise SystemExit('Files above 25 MiB remain: '+', '.join(large))
    print('Upload the contents of submission/ to the separate lab 2 repository.')
    print('Original local results were not changed. To restore split raw requests: python scripts/restore_raw.py')

if __name__=='__main__':main()
