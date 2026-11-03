import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from package_submission import split_raw
from restore_raw import restore

class PackagingTests(unittest.TestCase):
    def test_compressed_chunks_restore_exact_original_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            original=root/'original.json';target=root/'restored.json'
            data=dict(start_epoch=1234.125,rows=[dict(index=i,latency_ms=i*.013,pod='екземпляр') for i in range(7)],seconds=[])
            original.write_text(json.dumps(data,ensure_ascii=False,separators=(',',':')),encoding='utf-8')
            split_raw(original,root/'parts',chunk_size=3)
            self.assertEqual(restore(root/'parts',target),7)
            self.assertEqual(target.read_bytes(),original.read_bytes())
            with self.assertRaises(FileExistsError):restore(root/'parts',target)
