"""Durable raw Parquet and verified Binance historical archive ingestion."""
from pathlib import Path
import csv,hashlib,io,zipfile
from tank.util import canonical_json,iso,new_id,parse_time

class ArchiveIntegrityError(ValueError): pass

class RawArchive:
    def __init__(self,root): self.root=Path(root)/'market'/'raw'
    def write(self,instrument_id,kind,payload,*,received_ts=None):
        import pyarrow as pa
        import pyarrow.parquet as pq
        ts=iso(received_ts);day=ts[:10];safe=''.join(c if c.isalnum() or c in '-_.' else '_' for c in instrument_id)
        directory=self.root/day/safe/kind;directory.mkdir(parents=True,exist_ok=True)
        path=directory/(new_id('raw')+'.parquet');tmp=path.with_suffix('.tmp')
        row={'instrument_id':instrument_id,'kind':kind,'received_ts':ts,'payload_json':canonical_json(payload)}
        pq.write_table(pa.Table.from_pylist([row]),tmp,compression='zstd');tmp.replace(path)
        return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'received_ts':ts}

    @staticmethod
    def read_verified(path,checksum):
        import pyarrow.parquet as pq
        data=Path(path).read_bytes()
        if hashlib.sha256(data).hexdigest()!=checksum:raise ArchiveIntegrityError('raw Parquet checksum mismatch')
        return pq.read_table(io.BytesIO(data)).to_pylist()

def verify_binance_zip(payload,checksum_text,*,filename,kind,expected_unit=None,daily=True):
    parts=checksum_text.strip().split()
    if len(parts)!=2 or parts[1].lstrip('*')!=filename:raise ArchiveIntegrityError('checksum filename mismatch')
    if hashlib.sha256(payload).hexdigest().lower()!=parts[0].lower():raise ArchiveIntegrityError('archive checksum mismatch')
    with zipfile.ZipFile(io.BytesIO(payload)) as z:
        members=[n for n in z.namelist() if not n.endswith('/')]
        if len(members)!=1:raise ArchiveIntegrityError('archive must contain one CSV')
        rows=list(csv.reader(io.StringIO(z.read(members[0]).decode('utf-8-sig'))))
    if not rows:raise ArchiveIntegrityError('empty archive')
    header=None
    try:int(rows[0][0])
    except ValueError:header=rows.pop(0)
    if not rows:raise ArchiveIntegrityError('archive has no data rows')
    if kind=='metrics':
        if header is None:raise ArchiveIntegrityError('metrics header absent')
        try:index=header.index('create_time')
        except ValueError:raise ArchiveIntegrityError('metrics timestamp column absent')
        stamps=[int(parse_time(r[index]).timestamp()*1000) for r in rows];unit='MS';step=300000;expected=288
    else:
        index=5 if kind=='aggTrades' else 0
        stamps=[int(r[index]) for r in rows];unit='US' if stamps[0]>10**14 else 'MS';step=60000000 if unit=='US' else 60000;expected=1440
    if expected_unit is not None and unit!=expected_unit:raise ArchiveIntegrityError(f'timestamp unit {unit} != semantics {expected_unit}')
    gaps=[];duplicates=[]
    values=[int(r[0]) for r in rows] if kind=='aggTrades' else stamps
    increment=1 if kind=='aggTrades' else step
    for previous,current in zip(values,values[1:]):
        if current==previous:duplicates.append(current)
        elif current!=previous+increment:gaps.append({'after':previous,'before':current})
    if daily and kind!='aggTrades' and len(rows)!=expected:gaps.append({'expected_rows':expected,'actual_rows':len(rows)})
    return {'rows':rows,'header':header,'timestamp_unit':unit,'gaps':gaps,'duplicates':duplicates,'complete':not gaps and not duplicates,'sha256':parts[0]}
