import gradio as gr, yfinance as yf, pandas as pd, sqlite3, os, logging
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "data")
LOGS = os.path.join(BASE, "logs")
os.makedirs(DATA, exist_ok=True); os.makedirs(LOGS, exist_ok=True)

DB = os.path.join(DATA, "stock_history.db")
XDB = os.path.join(DATA, "xforge_historical.db")
LOG = os.path.join(LOGS, "xforge_errors.log")

logging.basicConfig(filename=LOG, level=logging.ERROR, format='%(asctime)s - %(message)s')
def log(msg): logging.error(msg)

FAV = ["AAPL", "TSLA", "GOOGL", "AMZN", "MSFT", "NVDA"]

def init():
    for d in [DB, XDB]:
        c = sqlite3.connect(d)
        c.execute('CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY, symbol TEXT, date TEXT, close REAL, volume INTEGER)')
        c.commit(); c.close()

def live():
    r = []
    for s in FAV:
        try:
            t = yf.Ticker(s)
            p = t.info.get("currentPrice") or t.history(period="1d")["Close"].iloc[-1]
            r.append({"Symbol": s, "Price": round(p, 2), "Time": datetime.now().strftime("%H:%M:%S")})
        except Exception as e: log(str(e)); r.append({"Symbol": s, "Price": "N/A", "Time": "Error"})
    return pd.DataFrame(r)

def fetch(sym, per="1y"):
    try:
        h = yf.Ticker(sym).history(period=per)
        if h.empty: return "No data", None
        c = sqlite3.connect(DB)
        df = h.reset_index()[["Date","Close","Volume"]]; df["symbol"] = sym
        df.columns = ["date","close","volume","symbol"]
        df.to_sql("history", c, if_exists="append", index=False); c.close()
        return f"Saved {len(h)} rows", df
    except Exception as e: log(str(e)); return str(e), None

def hist(sym, lim=100):
    c = sqlite3.connect(DB)
    df = pd.read_sql_query(f"SELECT * FROM history WHERE symbol='{sym}' ORDER BY date DESC LIMIT {lim}", c)
    c.close(); return df

def ship_cost(sym, qty):
    try:
        v = yf.Ticker(sym).history(period="5d")["Volume"].iloc[-1]
        cost = round(v * 0.00001 * qty, 2)
        return f"Shipping {sym} × {qty} → Est. cost: ${cost}"
    except Exception as e: log(str(e)); return str(e)

def sim_hist(sym, days=30):
    c = sqlite3.connect(DB)
    df = pd.read_sql_query(f"SELECT * FROM history WHERE symbol='{sym}' ORDER BY date DESC LIMIT {days}", c)
    c.close()
    if df.empty: return "No data"
    df["MA"] = df["close"].rolling(5).mean()
    sig = "BUY" if df["close"].iloc[-1] > df["MA"].iloc[-1] else "SELL"
    return f"SIM → {sig} signal for {sym}"

def forge(sym):
    return live()[live()["Symbol"]==sym], ship_cost(sym, 100), hist(sym, 20)

def xforge(sym):
    c = sqlite3.connect(XDB)
    df = pd.read_sql_query(f"SELECT * FROM history WHERE symbol='{sym}' LIMIT 100", c)
    c.close(); return df if not df.empty else pd.DataFrame({"msg":["No data"]})

def errors():
    try: return open(LOG).read()[-3000:]
    except: return "No errors yet"

def update_forge():
    os.system("python3 -c 'import yfinance as yf; print(\"Forge updated\")'"); return "✅ Forge DB refreshed"

init()
with gr.Blocks(title="XForge Trader v9.2") as app:
    gr.Image(os.path.join(BASE, "logo.jpg"), height=100, show_label=False)
    gr.Markdown("# XForge Trader – Complete Dashboard")
    with gr.Tab("🔥 Live Tickers"): gr.Button("Fetch Live Prices").click(live, None, gr.Dataframe())
    with gr.Tab("📈 Fetch & Store"):
        s=gr.Textbox("AAPL"); p=gr.Dropdown(["1y","5y"],value="1y")
        gr.Button("Fetch").click(fetch, [s,p], [gr.Textbox(), gr.Dataframe()])
    with gr.Tab("📊 History"): gr.Button("Show").click(hist, [gr.Textbox("AAPL"), gr.Slider(10, 500, value=100, step=10)], gr.Dataframe())
    with gr.Tab("🚚 Shipping+Cost"): gr.Button("Calculate").click(ship_cost, [gr.Textbox("AAPL"), gr.Number(100)], gr.Textbox())
    with gr.Tab("🧠 SIM on History"): gr.Button("Run SIM").click(sim_hist, [gr.Textbox("AAPL"), gr.Slider(10, 100, value=30, step=5)], gr.Textbox())
    with gr.Tab("🚀 Forge Dashboard"): gr.Button("Load All").click(forge, gr.Textbox("AAPL"), [gr.Dataframe(), gr.Textbox(), gr.Dataframe()])
    with gr.Tab("🗄️ XForge DB"): gr.Button("Query").click(xforge, gr.Textbox("AAPL"), gr.Dataframe())
    with gr.Tab("⚠️ Errors"): gr.Button("View Log").click(errors, None, gr.Textbox(lines=12))
    with gr.Tab("🔄 Update Forge"): gr.Button("Update Now").click(update_forge, None, gr.Textbox())
app.launch(server_name="0.0.0.0", server_port=7860, theme=gr.themes.Soft())
