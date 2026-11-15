import random
from src.utils import repository as repo
from src.utils.identity import now
from src.utils.security import hash_password
from src.services.statistics import analyze
from src.services import run as runs
from src.services import report as reports

def demo_rows():
 rng=random.Random(2301);rows=[]
 for group,scale in [('A',180),('B',330)]:
  for i in range(180):
   failure=rng.expovariate(1/scale);censor=rng.uniform(100,340);event=int(failure<=censor);duration=max(5,round(min(failure,censor)/5)*5)
   rows.append({'specimen_id':f'LC-{group}-{i+1:03d}','group':group,'duration_hours':float(duration),'event':event,'censored':1-event})
 return rows

def seed():
 repo.init_db()
 for role,name in [('admin','材料试验复核人'),('analyst','寿命分析员'),('viewer','工艺观察员')]:
  email=f'{role}@lichuan.test';repo.put('users',{'id':email,'email':email,'name':name,'role':role,'password_hash':hash_password('Life2026!')},insert_only=True)
 rows=demo_rows();repo.put('datasets',{'id':'ds-material','name':'11月试件寿命 · 360件','source':'material_life_202611.csv','rows':rows,'created_at':now()},insert_only=True)
 rule={'id':'rule-240h','name':'240小时行政观察截止','cutoff_hours':240.,'note':'独立右删失，截止时刻发生的事件仍计入；同刻先事件风险集再删失。','created_at':now()};repo.put('rules',rule,insert_only=True)
 study={'id':'study-process','name':'A/B加工批寿命复核','dataset_id':'ds-material','rule_id':'rule-240h','group_a':'A','group_b':'B','created_at':now()};repo.put('studies',study,insert_only=True)
 existing=[r for r in repo.all_rows('runs') if r['id']=='run-demo']
 if existing:
  run=existing[0]
  if run.get('status')=='completed' and run.get('result') and not (runs.ARTIFACT_DIR/(run.get('artifact') or 'run-demo.json')).is_file():
   # Seed never overwrites manual edits, but a missing artifact would make the
   # page look inconsistent; reconcile it from the stored result in place.
   meta=runs.write_artifact('run-demo',run['result'],token='seed')
   updated={**run,**meta}
   updated.setdefault('final_status','completed');updated.setdefault('attempts',1);updated.setdefault('max_attempts',runs.RUN_MAX_ATTEMPTS)
   repo.put('runs',updated)
 else:
  result=analyze(rows,rule,study);meta=runs.write_artifact('run-demo',result,token='seed')
  repo.put('runs',{'id':'run-demo','study_id':'study-process','status':'completed','result':result,'error':'','created_at':now(),'attempts':1,'max_attempts':runs.RUN_MAX_ATTEMPTS,'final_status':'completed','published_at':now(),'finished_at':now(),'started_at':now(),**meta})
 repo.put('reviews',{'id':'review-demo','run_id':'run-demo','comment':'独立右删失假设与同刻风险集已核对，中位未达到不填0。','status':'approved','reviewer':'admin@lichuan.test','created_at':now()},insert_only=True)
 if not any(r['id']=='report-demo' for r in repo.all_rows('reports')):
  # Persist the CSV before the row so the demo report is downloadable too.
  reports._write_report_file('report-demo',reports.render('report-demo','砾川材料A/B寿命风险集复核册',repo.get('runs','run-demo')))
  repo.put('reports',{'id':'report-demo','run_id':'run-demo','review_id':'review-demo','title':'砾川材料A/B寿命风险集复核册','created_at':now()},insert_only=True)
 return {t:len(repo.all_rows(t)) for t in ('users','datasets','rules','studies','runs','reviews','reports')}

