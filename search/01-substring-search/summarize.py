#!/usr/bin/env python3
"""Summarize inclusive root buffers; do not sum parent and child counters."""
from pathlib import Path
import collections,json,statistics
base=Path(__file__).resolve().parent
rows=[json.loads(line) for line in (base/'build/search-evidence/plans.jsonl').read_text().splitlines()]
groups=collections.defaultdict(list)
for row in rows: groups[(row['size'],row['term'],row['kind'],row['phase'])].append(row['plan'][0])
def walk(node):
    yield node
    for child in node.get('Plans',[]):yield from walk(child)
print('rows\tterm\tkind\tphase\tmedian_ms\troot_buffers\tread_buffers\tplan')
for key,values in groups.items():
    middle=sorted(values,key=lambda v:v['Execution Time'])[len(values)//2]
    root=middle['Plan']
    nodes=list(walk(root))
    brief=' > '.join(n['Node Type']+(':'+n['Index Name'] if 'Index Name'in n else '') for n in nodes)
    print('\t'.join(map(str,(*key,statistics.median(v['Execution Time'] for v in values),root.get('Shared Hit Blocks',0)+root.get('Shared Read Blocks',0),root.get('Shared Read Blocks',0),brief))))
