"""Render PropBetIQ marketing templates.

  python render.py reel  spec.json  out.mp4            # 1080x1080 shot-chart reel (38s, silent AAC track)
  python render.py card  card.json  out.png            # 1080x1350 static data card
  python render.py still spec.json  out.png --t 13     # one frame of the reel (for checking layout)

Needs: playwright (Chromium) and ffmpeg.
"""
import asyncio, base64, json, os, pathlib, shutil, subprocess, sys, tempfile
from playwright.async_api import async_playwright

HERE = pathlib.Path(__file__).resolve().parent
TPL = HERE.parent / "templates"

async def run(kind, spec, out, t=None):
    tpl = TPL / ("shotchart_reel.html" if kind in ("reel", "still") else "data_card.html")
    size = (1080, 1080) if kind in ("reel", "still") else (1080, 1350)
    async with async_playwright() as p:
        exe = os.environ.get("PW_CHROMIUM")
        b = await p.chromium.launch(args=["--allow-file-access-from-files"], **({"executable_path": exe} if exe else {}))
        pg = await b.new_page(viewport={"width": size[0], "height": size[1]})
        errs = []; pg.on("pageerror", lambda e: errs.append(str(e)))
        await pg.goto(tpl.as_uri()); await pg.evaluate("d=>init(d)", spec)
        if kind == "card":
            await pg.screenshot(path=out)
        elif kind == "still":
            await pg.evaluate(f"render({t or 13})"); await pg.screenshot(path=out)
        else:
            dur = await pg.evaluate("T.end+T.fade+3.8"); fps = 30
            tmp = tempfile.mkdtemp()
            for i in range(int(dur * fps)):
                d = await pg.evaluate(f"(render({i/fps}), c.toDataURL('image/png'))")
                pathlib.Path(tmp, f"f{i:05d}.png").write_bytes(base64.b64decode(d.split(",")[1]))
            subprocess.run(["ffmpeg", "-y", "-v", "error", "-framerate", str(fps), "-i", f"{tmp}/f%05d.png",
                            "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo", "-map", "0:v", "-map", "1:a",
                            "-c:v", "libx264", "-preset", "slow", "-profile:v", "high", "-level:v", "4.1", "-pix_fmt", "yuv420p",
                            "-r", str(fps), "-g", "60", "-b:v", "9M", "-maxrate", "12M", "-bufsize", "18M",
                            "-colorspace", "bt709", "-color_primaries", "bt709", "-color_trc", "bt709", "-vf", "setsar=1",
                            "-c:a", "aac", "-b:a", "128k", "-shortest", "-movflags", "+faststart", out], check=True)
            shutil.rmtree(tmp)
        await b.close()
        if errs: sys.exit("Template errors: " + "; ".join(errs[:3]))
    print("wrote", out)

if __name__ == "__main__":
    kind, spec, out = sys.argv[1], json.load(open(sys.argv[2])), sys.argv[3]
    t = float(sys.argv[sys.argv.index("--t") + 1]) if "--t" in sys.argv else None
    asyncio.run(run(kind, spec, out, t))
