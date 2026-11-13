"""Render the "If only the Xth quarter counted" reel (1080x1920, ~21.6s, silent track for music).
Usage: python3 render_if_only.py <data.json> <out.mp4> [stills]
"""
import os,sys,json,base64,subprocess
from playwright.sync_api import sync_playwright
data,out=sys.argv[1],sys.argv[2];stills=len(sys.argv)>3
tpl=os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','templates','if_only_quarters.html')
TL={'starts':[0,3.6,7.2,10.8,14.4],'end':18.6};DUR=21.6
kw={'executable_path':os.environ['PW_CHROMIUM']} if os.environ.get('PW_CHROMIUM') else {}
with sync_playwright() as p:
    b=p.chromium.launch(**kw);pg=b.new_page(viewport={'width':1080,'height':1920})
    pg.goto('file://'+os.path.abspath(tpl));pg.evaluate('([d,t])=>setup(d,t)',[json.load(open(data)),TL]);pg.wait_for_timeout(400)
    grab=lambda q:base64.b64decode(pg.evaluate(f"document.getElementById('c').toDataURL('image/jpeg',{q})").split(',')[1])
    if stills:
        for t in [3.0,6.6,10.2,13.8,17.5,20]:
            pg.evaluate('t=>drawScene(0,t)',t);open(f'/tmp/ifonly_{t}.jpg','wb').write(grab(.85))
        print('stills in /tmp/ifonly_*.jpg');b.close();sys.exit()
    ff=subprocess.Popen(['ffmpeg','-y','-loglevel','error','-f','image2pipe','-framerate','30','-c:v','mjpeg','-i','-','-c:v','libx264','-crf','18','-preset','veryfast','-pix_fmt','yuv420p','-r','30','/tmp/ifonly_v.mp4'],stdin=subprocess.PIPE)
    for k in range(round(DUR*30)):
        pg.evaluate('t=>drawScene(0,t)',k/30);ff.stdin.write(grab(.92))
    ff.stdin.close();assert ff.wait()==0;b.close()
subprocess.run(['ffmpeg','-y','-loglevel','error','-i','/tmp/ifonly_v.mp4','-f','lavfi','-t',str(DUR),'-i','anullsrc=r=48000:cl=stereo','-map','0:v','-map','1:a','-c:v','copy','-c:a','aac','-t',str(DUR),'-movflags','+faststart',out],check=True)
print('wrote',out)
