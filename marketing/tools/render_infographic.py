"""Render a weekly infographic (1080x1350 PNG).
Usage: python3 render_infographic.py <variant> <data.json> <out.png>
  variant: mon | sat (bars) | lolli | tiles | line
  data.json: same shape as examples/infographics_example_data.json
    {"wemby": {...monday player block...}, "sat": [4 prop cards]}
  For a new Monday player, write the same keys (shots, league, rows) — the template reads D.wemby.
  Text that is player-specific on Monday (name, headline, insight cards) lives in the template's
  monday() function: copy the template to /tmp and edit those strings for the new player.
"""
import os,sys,json
from playwright.sync_api import sync_playwright
variant,data,out=sys.argv[1],sys.argv[2],sys.argv[3]
tpl=os.environ.get('TEMPLATE',os.path.join(os.path.dirname(__file__),'..','templates','weekly_infographics.html'))
with sync_playwright() as p:
    kw={'executable_path':os.environ['PW_CHROMIUM']} if os.environ.get('PW_CHROMIUM') else {}
    b=p.chromium.launch(**kw);pg=b.new_page(viewport={'width':1080,'height':1350})
    pg.goto('file://'+os.path.abspath(tpl));pg.evaluate('d=>setData(d)',json.load(open(data)));pg.wait_for_timeout(400)
    pg.evaluate('w=>draw(w)',variant);pg.locator('#c').screenshot(path=out);b.close()
print('wrote',out)
