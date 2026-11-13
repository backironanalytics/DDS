"""Build data for templates/if_only_quarters.html ("If only the Xth quarter counted" reel).
Usage: python3 build_if_only.py <repo_root> "<Focus Player Name>" <out.json>
Prints each period's top 10 so you can write the 5 captions; then add them to the JSON:
  "captions": ["1st quarter: ...", "2nd quarter: ...", "First half: ...", "3rd quarter: ...", "4th quarter: ..."]
Rules: players with 6+ games this season; games with missing quarter data (q3_pts==0 while pts>=8)
are excluded by dropping that player; ranks use rounded (1 decimal) values so equal numbers share a rank ("T-").
"""
import json,glob,sys,datetime as dt
repo,focus,out=sys.argv[1],sys.argv[2],sys.argv[3]
d=json.load(open(sorted(glob.glob(f'{repo}/NBA/propbetiq_nba-*.json'))[-1]));cur=int(d['meta']['current_season_year'])
rows={};last=''
for p in d['players'].values():
    g=[x for x in p['games'] if x['season']==cur and x['season_type']=='regular']
    if len(g)<6 or any(x['q3_pts']==0 and x['pts']>=8 for x in g): continue
    n=len(g);f=lambda fn:round(sum(fn(x) for x in g)/n,1);last=max([last]+[x['date'] for x in g])
    rows[p['display_name']]=dict(team=p['current_team'],q1=f(lambda x:x['q1_pts']),q2=f(lambda x:x['h1_pts']-x['q1_pts']),h1=f(lambda x:x['h1_pts']),q3=f(lambda x:x['q3_pts']-x['h1_pts']),q4=f(lambda x:x['pts']-x['q3_pts']))
if focus not in rows: sys.exit(f'{focus} not eligible (needs 6+ games with quarter data)')
K=['q1','q2','h1','q3','q4'];P={}
srt={k:sorted(rows,key=lambda m:(-rows[m][k],m)) for k in K}
for k in K:
    for n in srt[k][:10]+[focus]: P.setdefault(n,{'team':rows[n]['team']})
for n in P:
    for k in K:
        i=srt[k].index(n);r=1+sum(1 for m in rows if rows[m][k]>rows[n][k]);tie=sum(1 for m in rows if rows[m][k]==rows[n][k])>1
        P[n][k]=rows[n][k];P[n]['s_'+k]=i+1 if i<10 else None;P[n]['r_'+k]=('T-' if tie else '')+str(r)
for k in K: print(k,[(n,rows[n][k],P[n]['r_'+k]) for n in srt[k][:10]],'| focus:',P[focus][k],P[focus]['r_'+k])
thru=dt.date.fromisoformat(last).strftime('%b %-d')
json.dump({'players':P,'focus':focus,'thru':thru,'captions':['','','','','']},open(out,'w'),ensure_ascii=False)
print('wrote',out,'(fill in captions)')
