"""
NerdAxe / NerdOctaxe — Master Dashboard
Compact table: supports any number of miners.
Type an IP in the text field and press Enter to add.

pip install websocket-client requests matplotlib
python nerd_dashboard.py
"""

import websocket, requests, threading, time, re
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
from matplotlib.widgets import TextBox
from datetime import datetime, timedelta

# ══════════════════════════════════════════════
#  INITIAL IPs  —  add or remove yours here
# ══════════════════════════════════════════════
INITIAL_IPS = []

POOL_TARGET   = 131_072
HIST_HOURS    = 8
GROUP_MIN     = 5
RADAR_MIN     = 10
API_POLL_SEC  = 5
WS_RECONNECT  = 8
# ══════════════════════════════════════════════

COLORS = [
    "#f7931a","#00d4ff","#39ff14","#bf5fff",
    "#ff4444","#ffdd00","#ff69b4","#00ffcc",
    "#ff8c42","#a8e6cf","#dcedc1","#ffd3b6",
]
BG, BG2, BG3 = "#080c10", "#111820", "#0d1520"
GRAY, GRAY2  = "#3a4a5a", "#6a7a8a"
ORANGE       = "#f7931a"

RANGES = [
    ("<Pool\nrejected",  lambda d: d < POOL_TARGET),
    ("131K–1M\nnormal",  lambda d: POOL_TARGET <= d <   1_000_000),
    ("1M–10M\ngood",     lambda d:   1_000_000 <= d <  10_000_000),
    ("10M–100M\nhuge",   lambda d:  10_000_000 <= d < 100_000_000),
    (">100M\nmonster",   lambda d: d >= 100_000_000),
]
LEVELS = [
    (POOL_TARGET, "Pool"),
    (1_000_000,   "1M"),
    (10_000_000,  "10M"),
    (100_000_000, "100M"),
]

RE_DIFF = re.compile(r'asic_result.*?\bdiff\s+([\d.]+)/', re.IGNORECASE)

lock          = threading.Lock()
MINERS        = []
miner_data    = {}
block_history = []
network_block = 0

# ─── helpers ──────────────────────────────────────────────────
def fmt_diff(v):
    try:
        v = float(str(v).replace("G","e9").replace("M","e6").replace("K","e3"))
    except:
        return str(v)
    if v >= 1e9:  return f"{v/1e9:.2f}G"
    if v >= 1e6:  return f"{v/1e6:.2f}M"
    if v >= 1e3:  return f"{v/1e3:.0f}K"
    return f"{v:.0f}"

def fmt_up(s):
    h, r = divmod(int(s), 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def init_miner(ip, color):
    miner_data[ip] = {
        "name": ip, "color": color,
        "times": [], "diffs": [], "shares": 0,
        "hashrate": 0.0, "temp": 0.0, "power": 0.0,
        "best_diff": "—", "uptime": 0,
        "ws_status": "Connecting...", "api_ok": False,
    }

# ─── WebSocket ────────────────────────────────────────────────
def make_ws(ip):
    def on_msg(ws, raw):
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        m = RE_DIFF.search(raw)
        if not m: return
        try: diff = float(m.group(1))
        except: return
        with lock:
            st = miner_data[ip]
            st["times"].append(datetime.now())
            st["diffs"].append(diff)
            st["shares"] += 1
            st["ws_status"] = "WS ✓"

    def on_err(ws, e):
        with lock: miner_data[ip]["ws_status"] = "WS ✗"

    def on_close(ws, *a):
        with lock: miner_data[ip]["ws_status"] = "Reconnecting..."
        time.sleep(WS_RECONNECT); run()

    def on_open(ws):
        with lock: miner_data[ip]["ws_status"] = "WS ✓"
        print(f"[{ip}] WebSocket connected")

    def run():
        websocket.WebSocketApp(
            f"ws://{ip}/api/ws",
            on_message=on_msg, on_error=on_err,
            on_close=on_close, on_open=on_open,
        ).run_forever(ping_interval=20, ping_timeout=10)

    threading.Thread(target=run, daemon=True, name=f"ws-{ip}").start()

# ─── REST polling ─────────────────────────────────────────────
def poll_api():
    while True:
        for m in list(MINERS):
            ip = m["ip"]
            try:
                d = requests.get(f"http://{ip}/api/system/info", timeout=4).json()
                with lock:
                    st = miner_data[ip]
                    st["name"]      = d.get("hostname") or d.get("deviceModel") or ip
                    st["hashrate"]  = float(d.get("hashRate", 0))
                    st["temp"]      = float(d.get("temp", 0))
                    st["power"]     = float(d.get("power", 0))
                    st["best_diff"] = str(d.get("bestDiff", d.get("bestSessionDiff", "—")))
                    st["uptime"]    = int(d.get("uptimeSeconds", 0))
                    st["api_ok"]    = True
            except:
                with lock: miner_data[ip]["api_ok"] = False
        time.sleep(API_POLL_SEC)

# ─── Bitcoin network ──────────────────────────────────────────
def poll_btc():
    global network_block
    try:
        blocks = requests.get("https://mempool.space/api/v1/blocks", timeout=10).json()
        blocks.reverse()
        with lock:
            for i, b in enumerate(blocks):
                h  = b["height"] + 1
                t0 = datetime.fromtimestamp(b["timestamp"])
                t1 = datetime.fromtimestamp(blocks[i+1]["timestamp"]) if i < len(blocks)-1 else None
                block_history.append({"block": h, "start": t0, "end": t1})
            if block_history:
                network_block = block_history[-1]["block"]
    except:
        print("Bitcoin: could not load initial block history.")

    while True:
        try:
            h = int(requests.get(
                "https://mempool.space/api/blocks/tip/height", timeout=5).text) + 1
            with lock:
                if h != network_block and h > 0:
                    now = datetime.now()
                    if block_history:
                        block_history[-1]["end"] = now
                    block_history.append({"block": h, "start": now, "end": None})
                    network_block = h
        except:
            pass
        time.sleep(30)

threading.Thread(target=poll_api, daemon=True, name="api").start()
threading.Thread(target=poll_btc, daemon=True, name="btc").start()

# ─── Initialize base miners ─────────────────────────────────────
for i, ip in enumerate(INITIAL_IPS):
    MINERS.append({"ip": ip})
    init_miner(ip, COLORS[i % len(COLORS)])
    make_ws(ip)
    print(f"[{ip}] Starting...")

# ═══════════════════════════════════════════════════════════════
#  FIGURE
# ═══════════════════════════════════════════════════════════════
fig = plt.figure(figsize=(17, 11), facecolor=BG)
fig.canvas.manager.set_window_title("NerdAxe — Master Dashboard")

# ── Title + status ───────────────────────────────────────────
fig.text(0.01, 0.99, "NerdAxe / NerdOctaxe — Master Dashboard",
         ha="left", va="top", color=ORANGE, fontsize=13, fontweight="bold")
status_txt = fig.text(0.99, 0.99, "",
                      ha="right", va="top", color=GRAY2, fontsize=8)

# ── Input field to add IP ──────────────────────────────────────
ax_input = fig.add_axes([0.70, 0.965, 0.10, 0.020])
text_box = TextBox(ax_input, "+ IP: ", color="#1a2a3a", hovercolor="#2a3a4a")
text_box.label.set_color("white"); text_box.label.set_fontsize(8)
text_box.text_disp.set_color("white")

def add_ip(ip_text):
    ip = ip_text.strip()
    if not ip or any(m["ip"] == ip for m in MINERS):
        text_box.set_val(""); return
    idx = len(MINERS)
    col = COLORS[idx % len(COLORS)]
    with lock:
        MINERS.append({"ip": ip})
        init_miner(ip, col)
    # add row to the table
    _add_table_row(ip, col, idx)
    make_ws(ip)
    print(f"[NEW] {ip} added")
    text_box.set_val("")

text_box.on_submit(add_ip)

# ═══════════════════════════════════════════════════════════════
#  METRICS TABLE (fixed area, dynamic text)
# ═══════════════════════════════════════════════════════════════
TABLE_B = 0.825   # bottom of the table
TABLE_H = 0.130   # total height of the table
NCOLS   = 10
HEADERS = ["Miner/Name", "IP", "Hashrate", "Temp", "Power",
           "Best Diff", "Shares", "Uptime", "API", "WS"]
# normalized X position for each column
COL_XS = [0.01, 0.13, 0.24, 0.34, 0.41,
           0.49, 0.61, 0.69, 0.79, 0.87]

ax_tbl = fig.add_axes([0.0, TABLE_B, 1.0, TABLE_H])
ax_tbl.set_facecolor(BG2)
ax_tbl.set_xlim(0, 1); ax_tbl.set_ylim(0, 1)
ax_tbl.set_xticks([]); ax_tbl.set_yticks([])
for sp in ax_tbl.spines.values():
    sp.set_edgecolor(GRAY)

# separator line under headers
HEADER_Y = 0.92
ROW_START = 0.83   # Y where the first row starts (below headers)
MAX_ROWS  = 10     # maximum visible rows simultaneously
ROW_H_NORM = (ROW_START) / MAX_ROWS  # height of each row in normalized coords

# Headers
for hdr, cx in zip(HEADERS, COL_XS):
    ax_tbl.text(cx + 0.003, HEADER_Y, hdr,
                color=GRAY2, fontsize=7.5, fontweight="bold", va="top")
ax_tbl.axhline(HEADER_Y - 0.13, color=GRAY, linewidth=0.7)

# Storage for Text objects per IP
table_rows = {}   # ip -> [Text, Text, ..., Text]  (NCOLS objects)

def _add_table_row(ip, color, idx):
    """Creates Text objects for a new row in the table."""
    visible_idx = idx % MAX_ROWS      # rotate if there are more than MAX_ROWS
    y = ROW_START - visible_idx * ROW_H_NORM - 0.02
    texts = []
    for j, cx in enumerate(COL_XS):
        clr = color if j == 0 else "white"
        fw  = "bold" if j == 0 else "normal"
        t = ax_tbl.text(cx + 0.003, y, "—",
                        color=clr, fontsize=8,
                        fontweight=fw, va="top")
        texts.append(t)
    # thin separator line between rows
    ax_tbl.axhline(y - ROW_H_NORM + 0.03,
                   color=GRAY, linewidth=0.4, alpha=0.4)
    table_rows[ip] = texts

# Create initial rows
for idx, m in enumerate(MINERS):
    _add_table_row(m["ip"], miner_data[m["ip"]]["color"], idx)

# ═══════════════════════════════════════════════════════════════
#  CHARTS  (below the table)
# ═══════════════════════════════════════════════════════════════
CHART_TOP = TABLE_B - 0.015
# Layout: 2 rows × 2 cols, row 0 full width = 8h history
from matplotlib.gridspec import GridSpec

gs = GridSpec(2, 2, figure=fig,
              left=0.05, right=0.98,
              top=CHART_TOP, bottom=0.06,
              hspace=0.40, wspace=0.22,
              height_ratios=[1, 1],
              width_ratios=[2.2, 1])

ax_8h      = fig.add_subplot(gs[0, :])   # 8h history — full row
ax_scatter = fig.add_subplot(gs[1, 0])   # 10 min radar
ax_hist    = fig.add_subplot(gs[1, 1])   # frequency histogram

for ax in [ax_8h, ax_scatter, ax_hist]:
    ax.set_facecolor(BG3)
    ax.tick_params(colors=GRAY2, labelsize=7)
    for sp in ax.spines.values():
        sp.set_edgecolor(GRAY)

# ─── drawing helpers ────────────────────────────────────────
def draw_blocks(ax, t0, t1):
    alt = [BG3, "#161e28"]
    with lock:
        for i, b in enumerate(block_history):
            bs = b["start"]; be = b["end"] if b["end"] else t1
            if be < t0 or bs > t1: continue
            ax.axvspan(max(bs, t0), min(be, t1),
                       facecolor=alt[i % 2], alpha=1.0, zorder=0)
            if bs >= t0:
                ax.axvline(bs, color="#2a3a4a", lw=1.2, ls=":", zorder=1)
                ax.text(bs, 0.99, f" {b['block']}",
                        color="#3a4a5a", fontsize=6,
                        transform=ax.get_xaxis_transform(),
                        rotation=90, ha="left", va="top")

def draw_grid(ax):
    ax.grid(True, which="major", axis="y",
            color="#1e2a38", lw=0.7, alpha=0.7, zorder=1)
    ax.grid(True, which="minor", axis="y",
            color="#161e28", ls=":", lw=0.4, alpha=0.5, zorder=1)
    ax.axhline(POOL_TARGET, color="#ffffff", lw=0.7, ls="--", alpha=0.2, zorder=2)
    for nivel, nombre in LEVELS:
        ax.axhline(nivel, color="#2a3a4a", lw=0.8, zorder=2)
        ax.text(0.001, nivel, f" {nombre}",
                color="#3a5a7a", fontsize=6.5,
                va="bottom", transform=ax.get_yaxis_transform())

# ═══════════════════════════════════════════════════════════════
#  UPDATE
# ═══════════════════════════════════════════════════════════════
def update(frame):
    now   = datetime.now()
    t0_8h = now - timedelta(hours=HIST_HOURS)
    t0_10 = now - timedelta(minutes=RADAR_MIN)

    # Clear old history
    with lock:
        for ip in miner_data:
            st = miner_data[ip]
            pairs = [(t, d) for t, d in zip(st["times"], st["diffs"])
                     if t >= t0_8h]
            st["times"] = [p[0] for p in pairs]
            st["diffs"]  = [p[1] for p in pairs]

    # ── Update table ──────────────────────────────────────
    with lock:
        for ip, texts in table_rows.items():
            if ip not in miner_data: continue
            st  = miner_data[ip]
            col = st["color"]
            ct  = ("#ff4444" if st["temp"] > 65
                   else (ORANGE if st["temp"] > 55 else "#39ff14"))
            cws = "#39ff14" if "✓" in st["ws_status"] else "#ff4444"
            cap = "#39ff14" if st["api_ok"] else "#ff4444"

            vals = [
                st["name"], ip,
                f"{st['hashrate']:,.0f} GH/s",
                f"{st['temp']:.1f} °C",
                f"{st['power']:.0f} W",
                fmt_diff(st["best_diff"]),
                str(st["shares"]),
                fmt_up(st["uptime"]),
                "✓" if st["api_ok"] else "✗",
                st["ws_status"],
            ]
            clrs = [col, GRAY2, col, ct, "#00d4ff",
                    "#bf5fff", "#39ff14", GRAY2, cap, cws]

            for t_obj, val, clr in zip(texts, vals, clrs):
                t_obj.set_text(val)
                t_obj.set_color(clr)

    # ── 8h Chart ────────────────────────────────────────────
    ax_8h.cla(); ax_8h.set_facecolor(BG3)
    ax_8h.tick_params(colors=GRAY2, labelsize=7)
    for sp in ax_8h.spines.values(): sp.set_edgecolor(GRAY)
    ax_8h.set_yscale("log")
    ax_8h.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda v, _: fmt_diff(v)))
    ax_8h.set_title(
        f"Max peaks grouped by {GROUP_MIN} min  |  Background = Bitcoin Blocks",
        color=GRAY2, fontsize=9, pad=5)
    draw_blocks(ax_8h, t0_8h, now)
    draw_grid(ax_8h)

    with lock:
        for m in list(MINERS):
            ip = m["ip"]; st = miner_data[ip]
            if not st["times"]: continue
            cub = {}
            for t, d in zip(st["times"], st["diffs"]):
                k = (t.timestamp() // (GROUP_MIN * 60)) * (GROUP_MIN * 60)
                if k not in cub or d > cub[k]: cub[k] = d
            if not cub: continue
            srt = sorted(cub)
            xt  = [datetime.fromtimestamp(k) for k in srt]
            yd  = [cub[k] for k in srt]
            ax_8h.plot(xt, yd, color=st["color"], lw=1.3, alpha=0.75,
                       zorder=3, label=st["name"])
            ax_8h.scatter(xt, yd, color=st["color"], s=14, alpha=0.9, zorder=4)
            ax_8h.scatter([xt[-1]], [yd[-1]], color=st["color"],
                          s=45, edgecolors="white", lw=0.7, zorder=5)

    ax_8h.set_xlim(t0_8h, now)
    ax_8h.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    plt.setp(ax_8h.get_xticklabels(), ha="center", fontsize=7, color=GRAY2)
    ax_8h.legend(loc="upper left", facecolor=BG2, edgecolor=GRAY,
                labelcolor="white", fontsize=7.5,
                ncol=max(1, min(len(MINERS), 6)), framealpha=0.85)

    # ── 10 min radar scatter ──────────────────────────────────
    ax_scatter.cla(); ax_scatter.set_facecolor(BG3)
    ax_scatter.tick_params(colors=GRAY2, labelsize=7)
    for sp in ax_scatter.spines.values(): sp.set_edgecolor(GRAY)
    ax_scatter.set_yscale("log")
    ax_scatter.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda v, _: fmt_diff(v)))
    ax_scatter.set_title(f"Live Radar — last {RADAR_MIN} min",
                         color=GRAY2, fontsize=9, pad=5)
    draw_blocks(ax_scatter, t0_10, now)
    draw_grid(ax_scatter)

    with lock:
        for m in list(MINERS):
            ip = m["ip"]; st = miner_data[ip]
            ts = [t for t in st["times"] if t >= t0_10]
            ds = [d for t, d in zip(st["times"], st["diffs"]) if t >= t0_10]
            if not ts: continue
            ax_scatter.plot(ts, ds, color=st["color"], lw=0.5, alpha=0.2, zorder=2)
            ax_scatter.scatter(ts, ds, color=st["color"], s=12,
                               alpha=0.8, zorder=3, label=st["name"])
            ax_scatter.scatter([ts[-1]], [ds[-1]], color=st["color"],
                               s=40, edgecolors="white", lw=0.7, zorder=5)
            ax_scatter.annotate(
                fmt_diff(ds[-1]),
                xy=(ts[-1], ds[-1]), xytext=(5, 3),
                textcoords="offset points",
                color=st["color"], fontsize=7, fontweight="bold")

    ax_scatter.set_xlim(t0_10, now)
    ax_scatter.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.setp(ax_scatter.get_xticklabels(),
            rotation=20, ha="right", fontsize=7, color=GRAY2)
    ax_scatter.legend(loc="upper left", facecolor=BG2, edgecolor=GRAY,
                    labelcolor="white", fontsize=7.5,
                    ncol=max(1, min(len(MINERS), 4)), framealpha=0.85)

    # ── Frequency histogram ────────────────────────────────
    ax_hist.cla(); ax_hist.set_facecolor(BG3)
    ax_hist.tick_params(colors=GRAY2, labelsize=7)
    for sp in ax_hist.spines.values(): sp.set_edgecolor(GRAY)
    ax_hist.spines["top"].set_visible(False)
    ax_hist.spines["right"].set_visible(False)
    ax_hist.grid(True, axis="y", color=GRAY, ls=":", alpha=0.4, zorder=0)
    ax_hist.set_title(f"Frequency by range (last {HIST_HOURS}h)",
                      color=GRAY2, fontsize=9, pad=5)

    nm = max(len(MINERS), 1)
    nr = len(RANGES)
    bw = 0.75 / nm

    with lock:
        for idx_m, m in enumerate(list(MINERS)):
            ip = m["ip"]; st = miner_data[ip]
            counts = [0] * nr
            for d in st["diffs"]:
                for idx_r, (_, cond) in enumerate(RANGES):
                    if cond(d): counts[idx_r] += 1; break
            xp = [x + (idx_m - nm / 2) * bw + bw / 2 for x in range(nr)]
            bars = ax_hist.bar(xp, counts, width=bw, color=st["color"],
                               alpha=0.85, label=st["name"], zorder=3)
            mx = max(counts) if counts else 1
            for b in bars:
                h = b.get_height()
                if h > 0:
                    ax_hist.text(
                        b.get_x() + b.get_width() / 2,
                        h + mx * 0.02 + 0.1,
                        str(int(h)), ha="center", va="bottom",
                        color="white", fontsize=7, fontweight="bold")

    ax_hist.set_xticks(range(nr))
    ax_hist.set_xticklabels([r[0] for r in RANGES], color="white", fontsize=7)
    ax_hist.legend(loc="upper right", facecolor=BG2, edgecolor=GRAY,
                   labelcolor="white", fontsize=7, framealpha=0.85)

    # ── Status bar ──────────────────────────────────────
    status_txt.set_text(
        f"Updated: {now:%H:%M:%S}  |  "
        f"Miners: {len(MINERS)}  |  "
        f"BTC Block: {network_block or '…'}")


update(0)
ani = animation.FuncAnimation(fig, update, interval=3000, cache_frame_data=False)
plt.show()