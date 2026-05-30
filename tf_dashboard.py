"""
NerdAxe / NerdOctaxe — Tkinter Master Dashboard
Hybrid GUI: Native Tkinter Table + Embedded Matplotlib Charts.
Supports hot-adding miners and live Bitcoin network sync.

pip install websocket-client requests matplotlib
python tf_dashboard.py
"""

import tkinter as tk
from tkinter import ttk
import websocket, requests, threading, time, re
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import matplotlib.animation as animation
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from datetime import datetime, timedelta
import os, json  # <--- SEGURO QUE YA TIENES OTROS IMPORTS, AÑADE ESTOS DOS Al FINAL DE LA LISTA

CONFIG_FILE = "config.json"

# Inicialización por defecto de las estadísticas de los pools
pool_stats = {
    "Datum": {"total_shares": 0, "high_shares": 0, "best_diff": 0.0, "sum_diff": 0.0},
    "Bassin": {"total_shares": 0, "high_shares": 0, "best_diff": 0.0, "sum_diff": 0.0},
    "Public Pool": {"total_shares": 0, "high_shares": 0, "best_diff": 0.0, "sum_diff": 0.0},
    "Parasite/Otros": {"total_shares": 0, "high_shares": 0, "best_diff": 0.0, "sum_diff": 0.0}
}

def load_config():
    """Lee las IPs y el histórico de los pools del almacenamiento local."""
    global pool_stats
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                data = json.load(f)
                # Cargamos el histórico guardado si existe, respetando la estructura
                saved_stats = data.get("pool_stats", {})
                for k in pool_stats:
                    if k in saved_stats:
                        pool_stats[k] = saved_stats[k]
                return data.get("ips", [])
        except:
            return []
    return []

def save_config():
    """Guarda las IPs actuales y las métricas acumuladas de los pools en el disco."""
    with lock:
        ips = [m["ip"] for m in MINERS]
        current_stats = dict(pool_stats)
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump({"ips": ips, "pool_stats": current_stats}, f, indent=4)
    except Exception as e:
        print(f"Error saving configuration: {e}")

INITIAL_IPS = load_config()

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
        "pool": "—", # <--- AÑADIMOS ESTO AQUÍ
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
            
            # --- ACUMULACIÓN EN EL HISTÓRICO DEL POOL ---
            pool_actual = st.get("pool", "—")
            
            # EL CANDADO: Si el pool sigue siendo "—" (la API aún no ha contestado), 
            # saltamos todo este bloque y NO guardamos el share en el ranking.
            if pool_actual != "—": 
                if pool_actual not in pool_stats:
                    pool_actual = "Parasite/Otros"
                    
                ps = pool_stats[pool_actual]
                ps["total_shares"] += 1
                ps["sum_diff"] += diff
                if diff > ps["best_diff"]:
                    ps["best_diff"] = diff
                if diff >= POOL_TARGET:  
                    ps["high_shares"] += 1
        
        # Guardamos en el JSON
        save_config()

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
                response = requests.get(f"http://{ip}/api/system/info", timeout=4)
                d = response.json()
                with lock:
                    st = miner_data[ip]
                    st["name"] = str(d.get("hostname") or d.get("deviceModel") or ip)
                    
                    try: st["hashrate"] = float(d.get("hashRate") or 0)
                    except: st["hashrate"] = 0.0
                    
                    try: st["temp"] = float(d.get("temp") or 0)
                    except: st["temp"] = 0.0
                    
                    try: st["power"] = float(d.get("power") or 0)
                    except: st["power"] = 0.0
                    
                    st["best_diff"] = str(d.get("bestDiff") or d.get("bestSessionDiff") or "—")
                    
                    try: st["uptime"] = int(d.get("uptimeSeconds") or 0)
                    except: st["uptime"] = 0
                    
                    # --- LÓGICA INTELIGENTE DEL POOL (FALLBACK DETECTION) ---
                    # 1. Buscamos el chivato en la raíz o dentro de la subcarpeta "stratum"
                    stratum_data = d.get("stratum", {})
                    usando_respaldo = d.get("usingFallback", stratum_data.get("usingFallback", False))
                    
                    # Filtro de seguridad: por si la placa lo envía como texto
                    if str(usando_respaldo).lower() == "true": 
                        usando_respaldo = True
                    elif str(usando_respaldo).lower() == "false": 
                        usando_respaldo = False
                    
                    # 2. Elegimos qué puerto leer según el estado de emergencia
                    if usando_respaldo:
                        puerto = str(d.get("fallbackStratumPort", ""))
                    else:
                        puerto = str(d.get("stratumPort", ""))
                    
                    # 3. Etiquetamos el pool
                    if not puerto or puerto == "None":
                        st["pool"] = "—"
                    elif "23334" in puerto:
                        st["pool"] = "Datum"
                    elif "3456" in puerto:
                        st["pool"] = "Bassin"
                    elif "2018" in puerto:
                        st["pool"] = "Public Pool"
                    elif "42069" in puerto:
                        st["pool"] = "Parasite"
                    else:
                        if usando_respaldo:
                            st["pool"] = f"Respaldo ({puerto})"
                        else:
                            st["pool"] = f"Otro ({puerto})"
                    # --------------------------------------------------------
                    
                    st["api_ok"] = True
            except Exception as e:
                with lock: 
                    if ip in miner_data:
                        miner_data[ip]["api_ok"] = False
        
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

# ═══════════════════════════════════════════════════════════════
#  TKINTER UI INITIALIZATION
# ═══════════════════════════════════════════════════════════════
root = tk.Tk()
root.title("NerdAxe — Master Dashboard")
root.geometry("1500x900")
root.configure(bg=BG)

# --- Top Control Frame ---
ctrl_frame = tk.Frame(root, bg=BG, pady=10)
ctrl_frame.pack(fill=tk.X, side=tk.TOP, padx=15)

title_lbl = tk.Label(ctrl_frame, text="NerdAxe / NerdOctaxe — Master Dashboard", fg=ORANGE, bg=BG, font=("Arial", 14, "bold"))
title_lbl.pack(side=tk.LEFT)

# Add IP widgets
add_frame = tk.Frame(ctrl_frame, bg=BG)
add_frame.pack(side=tk.RIGHT)

ip_lbl = tk.Label(add_frame, text="Add IP: ", fg="white", bg=BG, font=("Arial", 9, "bold"))
ip_lbl.pack(side=tk.LEFT, padx=5)

ip_entry = tk.Entry(add_frame, bg=BG2, fg="white", insertbackground="white", font=("Arial", 9), width=18, borderwidth=1, relief="solid")
ip_entry.pack(side=tk.LEFT, padx=5)

def submit_ip(event=None):
    ip_text = ip_entry.get().strip()
    if not ip_text: return
    with lock:
        if any(m["ip"] == ip_text for m in MINERS):
            ip_entry.delete(0, tk.END); return
        idx = len(MINERS)
        col = COLORS[idx % len(COLORS)]
        MINERS.append({"ip": ip_text})
        init_miner(ip_text, col)
    
    # ─── ¡AÑADE ESTA LÍNEA AQUÍ! ──────────────────────────────
    save_config()  # Guarda los cambios en el archivo al instante
    # ══════════════════════════════════════════════════════════
    
    make_ws(ip_text)
    print(f"[NEW] {ip_text} added via UI")
    ip_entry.delete(0, tk.END)

ip_entry.bind("<Return>", submit_ip)

add_btn = tk.Button(add_frame, text="Connect", bg=GRAY, fg="white", font=("Arial", 8, "bold"), activebackground=ORANGE, command=submit_ip, borderwidth=0, padx=10)
add_btn.pack(side=tk.LEFT, padx=5)

# --- Native Treeview Table ---
style = ttk.Style()
style.theme_use("clam")
style.configure("Treeview", background=BG2, fieldbackground=BG2, foreground="white", borderwidth=0, font=("Arial", 9), rowheight=24)
style.configure("Treeview.Heading", background=BG3, foreground=GRAY2, font=("Arial", 9, "bold"), borderwidth=0)
style.map("Treeview", background=[('selected', GRAY)])

HEADERS = ["Miner/Name", "IP Address", "Pool", "Hashrate", "Temperature", "Power Consumption", "Best Difficulty", "Total Shares", "Uptime", "API Status", "WS Status"]
# --- Tabla Nativa de Mineros (Ya la tienes) ---
tree = ttk.Treeview(root, columns=HEADERS, show="headings", height=5)
tree.pack(fill=tk.X, padx=15, pady=5)
# ... (deja el código de configuración del primer tree tal como está)

# --- NUEVA TABLA NATIVA PARA EL RANKING DE POOLS ---
POOL_HEADERS = ["Pool Name", "Total Shares", "High-Diff Shares", "Max Difficulty", "Average Difficulty", "Luck / Efficiency"]
tree_pools = ttk.Treeview(root, columns=POOL_HEADERS, show="headings", height=4)
tree_pools.pack(fill=tk.X, padx=15, pady=10)

for ph in POOL_HEADERS:
    tree_pools.heading(ph, text=ph, anchor=tk.W)
    tree_pools.column(ph, width=120, anchor=tk.W)
tree_pools.column("Pool Name", width=200)

for h in HEADERS:
    tree.heading(h, text=h, anchor=tk.W)
    tree.column(h, width=110, anchor=tk.W)
tree.column("Miner/Name", width=180) # Make name wider

# --- Embedded Matplotlib Figures Setup ---
fig = Figure(figsize=(16, 6), facecolor=BG)
gs = fig.add_gridspec(2, 2, left=0.05, right=0.98, top=0.93, bottom=0.08, hspace=0.45, wspace=0.22, height_ratios=[1, 1], width_ratios=[2.2, 1])

ax_8h      = fig.add_subplot(gs[0, :])
ax_scatter = fig.add_subplot(gs[1, 0])
ax_hist    = fig.add_subplot(gs[1, 1])

for ax in [ax_8h, ax_scatter, ax_hist]:
    ax.set_facecolor(BG3)
    ax.tick_params(colors=GRAY2, labelsize=7)
    for sp in ax.spines.values(): sp.set_edgecolor(GRAY)

canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

# --- Bottom Status Bar ---
status_lbl = tk.Label(root, text="Initializing...", fg=GRAY2, bg=BG3, font=("Arial", 8), anchor=tk.W, padx=15, pady=4)
status_lbl.pack(fill=tk.X, side=tk.BOTTOM)

# ─── Initialize base miners ─────────────────────────────────────
for i, ip in enumerate(INITIAL_IPS):
    MINERS.append({"ip": ip})
    init_miner(ip, COLORS[i % len(COLORS)])
    make_ws(ip)
    print(f"[{ip}] Starting...")

# ─── drawing helpers ────────────────────────────────────────
def draw_blocks(ax, t0, t1):
    alt = [BG3, "#161e28"]
    with lock:
        for i, b in enumerate(block_history):
            bs = b["start"]; be = b["end"] if b["end"] else t1
            if be < t0 or bs > t1: continue
            ax.axvspan(max(bs, t0), min(be, t1), facecolor=alt[i % 2], alpha=1.0, zorder=0)
            if bs >= t0:
                ax.axvline(bs, color="#2a3a4a", lw=1.2, ls=":", zorder=1)
                ax.text(bs, 0.99, f" {b['block']}", color="#3a4a5a", fontsize=6, transform=ax.get_xaxis_transform(), rotation=90, ha="left", va="top")

def draw_grid(ax):
    ax.grid(True, which="major", axis="y", color="#1e2a38", lw=0.7, alpha=0.7, zorder=1)
    ax.grid(True, which="minor", axis="y", color="#161e28", ls=":", lw=0.4, alpha=0.5, zorder=1)
    ax.axhline(POOL_TARGET, color="#ffffff", lw=0.7, ls="--", alpha=0.2, zorder=2)
    for nivel, nombre in LEVELS:
        ax.axhline(nivel, color="#2a3a4a", lw=0.8, zorder=2)
        ax.text(0.001, nivel, f" {nombre}", color="#3a5a7a", fontsize=6.5, va="bottom", transform=ax.get_yaxis_transform())

# ═══════════════════════════════════════════════════════════════
#  UPDATE LOOP
# ═══════════════════════════════════════════════════════════════
def update(frame):
    now   = datetime.now()
    t0_8h = now - timedelta(hours=HIST_HOURS)
    t0_10 = now - timedelta(minutes=RADAR_MIN)

    # Clear old history
    with lock:
        for ip in miner_data:
            st = miner_data[ip]
            pairs = [(t, d) for t, d in zip(st["times"], st["diffs"]) if t >= t0_8h]
            st["times"] = [p[0] for p in pairs]
            st["diffs"] = [p[1] for p in pairs]

    # ── Update Native Treeview Table ──────────────────────────
    with lock:
        for ip in list(miner_data.keys()):
            st = miner_data[ip]
            
            # Insert item if it doesn't exist in GUI tree
            if not tree.exists(ip):
                tree.insert("", "end", iid=ip, values=(st["name"], ip, "—", "—", "—", "—", "—", "0", "—", "✗", "Connecting..."))
            
            # Format and set data strings
            hashrate_str = f"{st['hashrate']:,.0f} GH/s" if st["api_ok"] else "—"
            temp_str     = f"{st['temp']:.1f} °C" if st["api_ok"] else "—"
            power_str    = f"{st['power']:.0f} W" if st["api_ok"] else "—"
            best_diff_str= fmt_diff(st["best_diff"])
            shares_str   = str(st["shares"])
            uptime_str   = fmt_up(st["uptime"]) if st["api_ok"] else "—"
            api_str      = "✓ OK" if st["api_ok"] else "✗ Down"
            ws_str       = st["ws_status"]
            pool_str     = st["pool"] # <--- CAPTURAMOS EL DATO

            # LO AÑADIMOS AQUÍ COMO TERCER ELEMENTO:
            tree.item(ip, values=(st["name"], ip, pool_str, hashrate_str, temp_str, power_str, best_diff_str, shares_str, uptime_str, api_str, ws_str))
    # ── 8h Chart ────────────────────────────────────────────
    ax_8h.cla(); ax_8h.set_facecolor(BG3)
    ax_8h.tick_params(colors=GRAY2, labelsize=7)
    for sp in ax_8h.spines.values(): sp.set_edgecolor(GRAY)
    ax_8h.set_yscale("log")
    ax_8h.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_diff(v)))
    ax_8h.set_title(f"Max peaks grouped by {GROUP_MIN} min  |  Background = Bitcoin Blocks", color=GRAY2, fontsize=8, pad=3)
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
            ax_8h.plot(xt, yd, color=st["color"], lw=1.3, alpha=0.75, zorder=3, label=st["name"])
            ax_8h.scatter(xt, yd, color=st["color"], s=14, alpha=0.9, zorder=4)
            ax_8h.scatter([xt[-1]], [yd[-1]], color=st["color"], s=45, edgecolors="white", lw=0.7, zorder=5)

    ax_8h.set_xlim(t0_8h, now)
    ax_8h.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    plt.setp(ax_8h.get_xticklabels(), ha="center", fontsize=7, color=GRAY2)
    if MINERS:
        ax_8h.legend(loc="upper left", facecolor=BG2, edgecolor=GRAY, labelcolor="white", fontsize=7, ncol=max(1, min(len(MINERS), 6)), framealpha=0.85)
    # ── 10 min radar scatter ──────────────────────────────────
    ax_scatter.cla(); ax_scatter.set_facecolor(BG3)
    ax_scatter.tick_params(colors=GRAY2, labelsize=7)
    for sp in ax_scatter.spines.values(): sp.set_edgecolor(GRAY)
    ax_scatter.set_yscale("log")
    ax_scatter.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: fmt_diff(v)))
    ax_scatter.set_title(f"Live Radar — last {RADAR_MIN} min", color=GRAY2, fontsize=8, pad=3)
    draw_blocks(ax_scatter, t0_10, now)
    draw_grid(ax_scatter)

    with lock:
        for m in list(MINERS):
            ip = m["ip"]; st = miner_data[ip]
            ts = [t for t in st["times"] if t >= t0_10]
            ds = [d for t, d in zip(st["times"], st["diffs"]) if t >= t0_10]
            if not ts: continue
            ax_scatter.plot(ts, ds, color=st["color"], lw=0.5, alpha=0.2, zorder=2)
            ax_scatter.scatter(ts, ds, color=st["color"], s=12, alpha=0.8, zorder=3, label=st["name"])
            ax_scatter.scatter([ts[-1]], [ds[-1]], color=st["color"], s=40, edgecolors="white", lw=0.7, zorder=5)
            ax_scatter.annotate(fmt_diff(ds[-1]), xy=(ts[-1], ds[-1]), xytext=(5, 3), textcoords="offset points", color=st["color"], fontsize=7, fontweight="bold")

    ax_scatter.set_xlim(t0_10, now)
    ax_scatter.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    plt.setp(ax_scatter.get_xticklabels(), rotation=15, ha="right", fontsize=7, color=GRAY2)
    if MINERS:
        ax_scatter.legend(loc="upper left", facecolor=BG2, edgecolor=GRAY, labelcolor="white", fontsize=7, ncol=max(1, min(len(MINERS), 4)), framealpha=0.85)
    # ── Frequency histogram ────────────────────────────────
    ax_hist.cla(); ax_hist.set_facecolor(BG3)
    ax_hist.tick_params(colors=GRAY2, labelsize=7)
    for sp in ax_hist.spines.values(): sp.set_edgecolor(GRAY)
    ax_hist.spines["top"].set_visible(False)
    ax_hist.spines["right"].set_visible(False)
    ax_hist.grid(True, axis="y", color=GRAY, ls=":", alpha=0.4, zorder=0)
    ax_hist.set_title(f"Frequency by range (last {HIST_HOURS}h)", color=GRAY2, fontsize=8, pad=3)

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
            bars = ax_hist.bar(xp, counts, width=bw, color=st["color"], alpha=0.85, label=st["name"], zorder=3)
            mx = max(counts) if counts else 1
            for b in bars:
                h = b.get_height()
                if h > 0:
                    ax_hist.text(b.get_x() + b.get_width() / 2, h + mx * 0.02 + 0.1, str(int(h)), ha="center", va="bottom", color="white", fontsize=7, fontweight="bold")

    ax_hist.set_xticks(range(nr))
    ax_hist.set_xticklabels([r[0] for r in RANGES], color="white", fontsize=6.5)
    if MINERS:
        ax_hist.legend(loc="upper right", facecolor=BG2, edgecolor=GRAY, labelcolor="white", fontsize=7, framealpha=0.85)

    # ── Actualizar Tabla de Ranking de Pools ──────────────────
    with lock:
        for p_name, ps in pool_stats.items():
            # Evitamos la división por cero si el pool no tiene shares aún
            total = ps["total_shares"]
            avg_diff = ps["sum_diff"] / total if total > 0 else 0.0
            luck_pct = (ps["high_shares"] / total * 100) if total > 0 else 0.0
            
            # Formateamos los valores para la interfaz
            max_diff_str = fmt_diff(ps["best_diff"]) if ps["best_diff"] > 0 else "—"
            avg_diff_str = fmt_diff(avg_diff) if avg_diff > 0 else "—"
            luck_str     = f"{luck_pct:.2f} %" if total > 0 else "0.00 %"
            
            # Insertar o actualizar la fila en la tabla de la interfaz
            if not tree_pools.exists(p_name):
                tree_pools.insert("", "end", iid=p_name, values=(p_name, total, ps["high_shares"], max_diff_str, avg_diff_str, luck_str))
            else:
                tree_pools.item(p_name, values=(p_name, total, ps["high_shares"], max_diff_str, avg_diff_str, luck_str))

    # ── Status bar update ──────────────────────────────────────
    status_lbl.configure(text=f" Last System Update: {now:%H:%M:%S}   |   Active Miners: {len(MINERS)}   |   Current Network Block Height: {network_block or 'Fetching...'}")
    canvas.draw()

# Run Matplotlib animation seamlessly mapped to Tkinter event loop
ani = animation.FuncAnimation(fig, update, interval=3000, cache_frame_data=False)
root.mainloop()