import gzip
import hashlib
import json
import os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def restore(folder,target):
    if target.exists():
        raise FileExistsError(f'{target} already exists; no replacement was performed')
    metadata=json.loads((folder/'metadata.json').read_text(encoding='utf-8'))
    rows=[]
    for name in metadata['parts']:
        if Path(name).name!=name:raise ValueError('Invalid part filename')
        with gzip.open(folder/name,'rt',encoding='utf-8') as source:rows.extend(json.load(source))
    if len(rows)!=metadata['row_count']:raise ValueError('Raw request count mismatch')
    data={key:(rows if key=='rows' else metadata['other_fields'][key]) for key in metadata['root_keys']}
    temporary=target.with_suffix('.restore.tmp')
    with temporary.open('w',encoding='utf-8',newline='') as out:
        json.dump(data,out,ensure_ascii=False,separators=(',',':'))
    if hashlib.sha256(temporary.read_bytes()).hexdigest()!=metadata['original_sha256']:
        temporary.unlink()
        raise ValueError('Restored bytes do not match the original SHA256')
    os.replace(temporary,target)
    return len(rows)

if __name__=='__main__':
    count=restore(ROOT/'results/noop-raw',ROOT/'results/noop.json')
    print(f'Restored {count} raw requests. Original SHA256 verified.')
