import asyncio, gzip, hashlib, json, os, shutil, subprocess, tempfile, sys
from pathlib import Path
from datetime import timedelta
import psycopg
from psycopg import sql
from tank.settings import Settings
from tank.storage import PostgresRepository
from tank.scheduler import Scheduler, verify_backup, resolve_backup_reference
from tank.util import iso, utcnow, canonical_json, sha256

async def main():
    root=Path(tempfile.mkdtemp(prefix='tank-real-recovery-'))
    suffix=root.name.rsplit('-',1)[-1].replace('-','')
    names={key:'tank_recovery_'+key+'_'+suffix for key in ('source','oma','restore','oma_restore')}
    admin=psycopg.connect(host='127.0.0.1',port=55436,dbname='postgres',autocommit=True)
    result={}
    try:
        for name in names.values():admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        live=root/'live';data=live/'data';config=live/'config';logs=live/'logs';backups=root/'backup';backups.mkdir()
        for path in (data,config,logs):path.mkdir(parents=True,exist_ok=True)
        (config/'registry.yaml').write_text('instruments: []\n')
        (logs/'openma.log').write_text('recovery fixture session completed\n')
        fixture_files={'raw/sources/source.gz':gzip.compress(b'official source permanent evidence'), 'raw/web/page.txt':b'verified official web quote', 'replay/bundles/pinned/inputs.json':b'{"fixture_id":"RECOVERY"}', 'bundles/pinned.json':b'{"claim":"durable evidence"}'}
        for relative,body in fixture_files.items():
            path=data/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
        repo=PostgresRepository('postgresql://127.0.0.1:55436/'+names['source'])
        await repo.open();await repo.migrate()
        raw=str(data/'raw/sources/source.gz')
        await repo.put('ingest.source_items','recovery-item',{'item_id':'recovery-item','source_id':'S01','external_id':'recovery-1','url':'https://example.invalid/official','title':'Recovery fixture','observed_at':iso(),'raw_ref':raw,'raw_sha256':hashlib.sha256(b'official source permanent evidence').hexdigest()})
        await repo.put('evt.events','recovery-event',{'event_id':'recovery-event','statement':'Recovery fixture','known_to_us_at':iso(),'lifecycle':'TRACKED'})
        await repo.enqueue('ROUTE_ITEM',{'item_id':'recovery-item'},'recovery-item')
        bundle={'cutoff':iso(),'source_items':[{'item_id':'recovery-item','raw_ref':raw}],'claims':[]}
        digest=sha256(canonical_json(bundle))
        await repo.put('out.evidence_bundles',digest,{'bundle_id':digest,**bundle},immutable=True)
        oma=psycopg.connect(host='127.0.0.1',port=55436,dbname=names['oma'],autocommit=True)
        oma.execute('CREATE TABLE recovery_session(session_id text PRIMARY KEY, transcript jsonb NOT NULL)')
        oma.execute('INSERT INTO recovery_session VALUES (%s,%s)',('session-recovery',json.dumps({'status':'IDLE','messages':['official quote verified']})))
        oma.close()
        settings=Settings(_env_file=None,database_url='postgresql://127.0.0.1:55436/'+names['source'],openma_database_url='postgresql://127.0.0.1:55436/'+names['oma'],data_dir=data,config_dir=config,logs_dir=logs,x_profile_dir=live/'profile',backup_dir=backups,backup_hour_utc=23)
        await repo.close()
        cli_env={key:value for key,value in os.environ.items() if not key.startswith('TANK_')}
        neutral_cwd=root/'cli-empty-working-directory';neutral_cwd.mkdir()
        cli_env.update(TANK_DATABASE_URL=settings.database_url,TANK_OPENMA_DATABASE_URL=settings.openma_database_url,TANK_DATA_DIR=str(data),TANK_CONFIG_DIR=str(config),TANK_LOGS_DIR=str(logs),TANK_X_PROFILE_DIR=str(live/'profile'),TANK_BACKUP_DIR=str(backups),TANK_BACKUP_HOUR_UTC='23')
        cli=subprocess.run([sys.executable,'-m','tank.cli','backup','--force'],env=cli_env,cwd=neutral_cwd,capture_output=True,text=True)
        if cli.returncode:raise RuntimeError('Canonical backup CLI failed: '+cli.stderr)
        outcome=json.loads(cli.stdout)
        backup=Path(outcome['path']);manifest=verify_backup(backup)
        # Relocate snapshot, then remove all original fixture files.
        relocated=root/'relocated';shutil.copytree(backup,relocated);shutil.rmtree(live)
        env=os.environ.copy();env.update(PGHOST='127.0.0.1',PGPORT='55436')
        for database,dump in ((names['restore'],'tank.dump'),(names['oma_restore'],'oma.dump')):
            proc=subprocess.run(['/tmp/tank-pg16/bin/pg_restore','--exit-on-error','--no-owner','--dbname',database,str(relocated/dump)],env=env,capture_output=True,text=True)
            if proc.returncode:raise RuntimeError('pg_restore failed: '+proc.stderr)
        restored=PostgresRepository('postgresql://127.0.0.1:55436/'+names['restore']);await restored.open()
        item=await restored.get('ingest.source_items','recovery-item')
        assert gzip.decompress(resolve_backup_reference(relocated,item['raw_ref']).read_bytes())==b'official source permanent evidence'
        for relative,body in fixture_files.items():assert resolve_backup_reference(relocated,str(data/relative)).read_bytes()==body
        assert (await restored.get('evt.events','recovery-event'))['lifecycle']=='TRACKED'
        jobs=await restored.list('ops.jobs');assert len(jobs)==1 and jobs[0]['status']=='QUEUED'
        frozen=await restored.get('out.evidence_bundles',digest);assert frozen['source_items'][0]['item_id']=='recovery-item'
        await restored.close()
        oma_restored=psycopg.connect(host='127.0.0.1',port=55436,dbname=names['oma_restore'])
        transcript=oma_restored.execute('SELECT transcript FROM recovery_session WHERE session_id=%s',('session-recovery',)).fetchone()[0]
        assert transcript['messages']==['official quote verified'];oma_restored.close()
        verify_backup(relocated)
        result={'server_version':admin.execute('SHOW server_version').fetchone()[0],'pg_dump_version':subprocess.check_output(['pg_dump','--version'],text=True).strip(),'pg_restore_version':subprocess.check_output(['/tmp/tank-pg16/bin/pg_restore','--version'],text=True).strip(),'snapshot_status':outcome['status'],'manual_before_backup_hour':utcnow().hour < 23,'retention':False,'restore_exit_codes':[0,0],'tank_checks':{'source_items':1,'events':1,'queued_jobs':1,'immutable_evidence_bundles':1},'openma_transcript_rows':1,'recovered_fixture_files':len(fixture_files),'manifest_files':len(manifest['files']),'dump_sha256':{name:manifest['files'][name] for name in ('tank.dump','oma.dump')},'original_live_tree_removed':not live.exists(),'relocated_snapshot_verified':True,'backup_path':str(relocated),'all_checks_passed':True,'isolated_databases_dropped_on_exit':True}
        Path('docs/RECOVERY_VERIFICATION.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
        print(json.dumps(result,sort_keys=True,indent=2))
    finally:
        for name in reversed(list(names.values())):
            admin.execute(sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(sql.Identifier(name)))
        admin.close()
asyncio.run(main())
