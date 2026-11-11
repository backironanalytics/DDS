"""Render an animated reel (1080x1920, 30fps) from a canvas template and mux a voiceover.
Template contract: window.ready() -> Promise; window.draw(sceneIndex, tSeconds) draws one frame.
Usage:
  python3 render_reel.py stills <template.html> "<d1,d2,...>"            # mid/end stills per scene -> /tmp/reel_still_<i>_<pct>.jpg
  python3 render_reel.py video  <template.html> "<d1,d2,...>" <vo.mp3|-> <out.mp4> [tempo]
Scene durations come from the voiceover: run
  ffmpeg -i vo.mp3 -af silencedetect=noise=-35dB:d=0.25 -f null - 2>&1 | grep silence_
and cut each scene where the narrator starts its line (divide by tempo if you speed the VO up, e.g. 1.08).
"""
import os,sys,base64,subprocess
from playwright.sync_api import sync_playwright
mode,tpl,durs=sys.argv[1],sys.argv[2],[float(x) for x in sys.argv[3].split(',')]
kw={'executable_path':os.environ['PW_CHROMIUM']} if os.environ.get('PW_CHROMIUM') else {}
with sync_playwright() as p:
    b=p.chromium.launch(**kw);pg=b.new_page(viewport={'width':1080,'height':1920})
    pg.goto('file://'+os.path.abspath(tpl));pg.evaluate('ready()');pg.wait_for_timeout(400)
    grab=lambda q:base64.b64decode(pg.evaluate(f"document.getElementById('c').toDataURL('image/jpeg',{q})").split(',')[1])
    if mode=='stills':
        for i,d in enumerate(durs,1):
            for f in (0.5,0.95):
                pg.evaluate('([i,t])=>draw(i,t)',[i,d*f]);open(f'/tmp/reel_still_{i}_{int(f*100)}.jpg','wb').write(grab(0.85))
        b.close();sys.exit()
    vo,out=sys.argv[4],sys.argv[5];tempo=float(sys.argv[6]) if len(sys.argv)>6 else 1.0
    files=[]
    for i,d in enumerate(durs,1):
        tmp=f'/tmp/scene{i}.tmp.mp4'
        ff=subprocess.Popen(['ffmpeg','-y','-loglevel','error','-f','image2pipe','-framerate','30','-c:v','mjpeg','-i','-','-c:v','libx264','-crf','18','-preset','veryfast','-pix_fmt','yuv420p','-r','30',tmp],stdin=subprocess.PIPE)
        for k in range(round(d*30)):
            pg.evaluate('([i,t])=>draw(i,t)',[i,k/30]);ff.stdin.write(grab(0.92))
        ff.stdin.close();assert ff.wait()==0;os.rename(tmp,f'/tmp/scene{i}.mp4');files.append(f'/tmp/scene{i}.mp4')
    b.close()
open('/tmp/scenes.txt','w').write(''.join(f"file '{f}'\n" for f in files))
subprocess.run(['ffmpeg','-y','-loglevel','error','-f','concat','-safe','0','-i','/tmp/scenes.txt','-c','copy','/tmp/joined.mp4'],check=True)
D=sum(round(d*30) for d in durs)/30
if vo=='-':
    subprocess.run(['ffmpeg','-y','-loglevel','error','-i','/tmp/joined.mp4','-f','lavfi','-t',str(D),'-i','anullsrc=r=48000:cl=stereo','-map','0:v','-map','1:a','-c:v','copy','-c:a','aac','-t',str(D),'-movflags','+faststart',out],check=True)
else:
    subprocess.run(['ffmpeg','-y','-loglevel','error','-i',vo,'-af',f'atempo={tempo},loudnorm=I=-14:TP=-1.5:LRA=11,apad=whole_dur={D}','-ar','48000','-ac','2','/tmp/vo.wav'],check=True)
    subprocess.run(['ffmpeg','-y','-loglevel','error','-i','/tmp/joined.mp4','-i','/tmp/vo.wav','-map','0:v','-map','1:a','-c:v','copy','-c:a','aac','-b:a','192k','-t',str(D),'-movflags','+faststart',out],check=True)
print('wrote',out,round(D,2),'s')
