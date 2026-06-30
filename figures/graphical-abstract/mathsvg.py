"""Render LaTeX math to recolorable, font-independent SVG path fragments
that can be spliced into a hand-built SVG. Uses matplotlib's mathtext
engine with Computer Modern, so output matches a LaTeX manuscript.
No system TeX install required (pip-only)."""
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib as mpl
import io, re, itertools, functools

mpl.rcParams["svg.fonttype"] = "path"     # glyphs -> vector paths (portable)
mpl.rcParams["mathtext.fontset"] = "cm"   # Computer Modern (LaTeX look)

SCALE = 0.62        # px per matplotlib pt; global size knob for all equations
_FS = 22            # render font size (constant -> uniform glyph size)
_uid = itertools.count()


@functools.lru_cache(maxsize=256)
def _prim(latex, color, fontsize):
    fig = plt.figure(figsize=(0.01, 0.01))
    fig.text(0, 0, f"${latex}$", fontsize=fontsize, color=color)
    buf = io.BytesIO()
    fig.savefig(buf, format="svg", bbox_inches="tight", pad_inches=0.0, transparent=True)
    plt.close(fig)
    s = buf.getvalue().decode()
    vb = re.search(r'viewBox="([\d.\s]+)"', s).group(1)
    _, _, vbw, vbh = map(float, vb.split())
    inner = s[s.find(">", s.find("<svg")) + 1: s.rfind("</svg>")]
    return inner, vb, vbw, vbh


class Math:
    __slots__ = ("inner", "vb", "w", "h")

    def __init__(self, latex, color="#16202e", scale=SCALE, fontsize=_FS):
        inner, vb, vbw, vbh = _prim(latex, color, fontsize)
        p = f"m{next(_uid)}-"                       # unique id namespace per instance
        inner = re.sub(r'id="([^"]+)"', lambda m: f'id="{p}{m.group(1)}"', inner)
        inner = re.sub(r'(xlink:href|href)="#([^"]+)"',
                       lambda m: f'{m.group(1)}="#{p}{m.group(2)}"', inner)
        self.inner, self.vb = inner, vb
        self.w, self.h = vbw * scale, vbh * scale

    def place(self, x, y, anchor="middle"):
        """y is the TOP of the math box. anchor in {start,middle,end} on x."""
        x0 = x - self.w / 2 if anchor == "middle" else (x - self.w if anchor == "end" else x)
        return (f'<svg x="{x0:.2f}" y="{y:.2f}" width="{self.w:.2f}" height="{self.h:.2f}" '
                f'viewBox="{self.vb}" xmlns:xlink="http://www.w3.org/1999/xlink" '
                f'preserveAspectRatio="xMidYMid meet" overflow="visible">{self.inner}</svg>')


def make(latex, color="#16202e", scale=SCALE, fontsize=_FS):
    return Math(latex, color, scale, fontsize)
