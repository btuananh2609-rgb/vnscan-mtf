"""
mtf_scanner.py — Multi-Timeframe Scanner
Phân tích MACD(12,26,9) + Stochastic(8,5,3) trên 3 khung: Tháng / Tuần / Ngày
Gửi email khi cả 3 khung đồng thuận tăng.
"""

import os
import time
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)

# ── VN100 chính xác 100 mã (VN30 + VN Midcap, cập nhật 2025) ──────────
VN100 = [
    # VN30 — 30 mã vốn hóa lớn nhất
    "VCB", "BID", "CTG", "MBB", "TCB", "ACB", "VPB", "HDB", "LPB", "STB",
    "VHM", "VIC", "VRE", "MSN", "VNM", "SAB", "MCH", "MWG", "PNJ", "FRT",
    "HPG", "GAS", "PLX", "POW", "GEX", "FPT", "VND", "SSI", "HCM", "VCI",
    # VN Midcap — 70 mã vốn hóa trung bình
    "AAA", "AGG", "ANV", "BCM", "BSR", "BWE", "CII", "CMG", "CTD", "CTR",
    "DCM", "DGC", "DGW", "DHC", "DIG", "DPG", "DPM", "DRC", "DXG", "EIB",
    "EVF", "GEG", "GMD", "HAH", "HAX", "HBC", "HDG", "HHV", "HSG", "HT1",
    "HVN", "IMP", "KBC", "KDH", "KOS", "LCG", "MSB", "NAB", "NKG", "NLG",
    "NVL", "OCB", "OIL", "PC1", "PDR", "PHR", "PME", "PVD", "PVS", "PVT",
    "QNS", "REE", "SBT", "SCS", "SHB", "SIP", "SZC", "TCH", "TDC", "TLG",
    "TPB", "VCG", "VGC", "VHC", "VIB", "VIX", "VJC", "VOS", "VSC", "VTP",
]

# ── DATA FETCHING ───────────────────────────────────────────────────────

def fetch_ohlcv(ticker: str, days: int = 730) -> Optional[pd.DataFrame]:
    """Lấy dữ liệu OHLCV từ VCI — đủ để tính cả khung tháng."""
    try:
        from vnstock import Quote
        # Đọc API key từ biến môi trường Render
        api_key = os.environ.get("VNSTOCK_API_KEY", "")
        if api_key:
            os.environ["VNSTOCK_API_KEY"] = api_key
        end   = datetime.today().strftime("%Y-%m-%d")
        start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        q  = Quote(symbol=ticker, source="VCI")
        df = q.history(start=start, end=end, interval="1D")
        if df is None or df.empty:
            return None
        df.columns = [c.lower() for c in df.columns]
        for old, new in {"tradingdate":"date","datetime":"date","time":"date"}.items():
            if old in df.columns:
                df.rename(columns={old:new}, inplace=True)
        df = df.sort_values("date").reset_index(drop=True)
        for col in ["open","high","low","close","volume"]:
            if col not in df.columns:
                return None
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        log.warning(f"Fetch lỗi {ticker}: {e}")
        return None


def resample_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Chuyển dữ liệu ngày → tuần (W-FRI)."""
    df = df.set_index("date")
    weekly = df.resample("W-FRI").agg({
        "open":   "first",
        "high":   "max",
        "low":    "min",
        "close":  "last",
        "volume": "sum",
    }).dropna().reset_index()
    return weekly


def resample_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Chuyển dữ liệu ngày → tháng (ME)."""
    df = df.set_index("date")
    monthly = df.resample("ME").agg({
        "open":   "first",
        "high":   "max",
        "low":    "min",
        "close":  "last",
        "volume": "sum",
    }).dropna().reset_index()
    return monthly


# ── INDICATORS ──────────────────────────────────────────────────────────

def compute_macd(close: pd.Series,
                 fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
    """
    MACD(12,26,9) — EMA-based.
    Tín hiệu: MACD line vừa cắt lên Signal line (crossover bullish).
    """
    ema_fast   = close.ewm(span=fast,   adjust=False).mean()
    ema_slow   = close.ewm(span=slow,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line= macd_line.ewm(span=signal, adjust=False).mean()
    histogram  = macd_line - signal_line

    if len(macd_line) < 2:
        return {"signal": False, "macd": None, "sig": None, "hist": None}

    macd_cur  = float(macd_line.iloc[-1])
    macd_prev = float(macd_line.iloc[-2])
    sig_cur   = float(signal_line.iloc[-1])
    sig_prev  = float(signal_line.iloc[-2])
    hist_cur  = float(histogram.iloc[-1])

    # Bullish crossover: MACD vừa cắt lên Signal
    crossover = (macd_prev <= sig_prev) and (macd_cur > sig_cur)
    # Đang trong vùng bullish (MACD > Signal, dù chưa cắt phiên này)
    bullish   = macd_cur > sig_cur

    return {
        "crossover": crossover,   # vừa cắt lên phiên gần nhất
        "bullish":   bullish,     # đang ở trên signal
        "signal":    crossover,   # tín hiệu chính để filter
        "macd":      round(macd_cur, 4),
        "sig":       round(sig_cur, 4),
        "hist":      round(hist_cur, 4),
    }


def compute_stoch(df: pd.DataFrame,
                  k_period: int = 8, k_smooth: int = 5, d_smooth: int = 3) -> dict:
    """
    Stochastic(8,5,3) — %K cắt lên %D, vùng dưới 80.
    """
    try:
        lowest  = df["low"].rolling(k_period).min()
        highest = df["high"].rolling(k_period).max()
        denom   = (highest - lowest).replace(0, np.nan)
        raw_k   = 100 * (df["close"] - lowest) / denom
        pct_k   = raw_k.rolling(k_smooth).mean()
        pct_d   = pct_k.rolling(d_smooth).mean()

        if pct_k.isna().iloc[-1] or pct_d.isna().iloc[-1]:
            return {"signal": False}

        k_cur  = float(pct_k.iloc[-1])
        d_cur  = float(pct_d.iloc[-1])
        k_prev = float(pct_k.iloc[-2])
        d_prev = float(pct_d.iloc[-2])

        crossover  = (k_prev <= d_prev) and (k_cur > d_cur)
        valid_zone = k_cur < 80
        bullish    = k_cur > d_cur

        zone = "Quá bán" if k_cur < 20 else "Trung lập" if k_cur < 80 else "Quá mua"

        return {
            "crossover":  crossover,
            "bullish":    bullish,
            "signal":     crossover and valid_zone,
            "valid_zone": valid_zone,
            "k":  round(k_cur, 1),
            "d":  round(d_cur, 1),
            "zone": zone,
        }
    except Exception as e:
        return {"signal": False, "error": str(e)}


# ── TIMEFRAME ANALYSIS ──────────────────────────────────────────────────

def analyze_timeframe(df: pd.DataFrame, label: str) -> dict:
    """Phân tích MACD + Stochastic cho một khung thời gian."""
    if df is None or len(df) < 30:
        return {
            "label":   label,
            "macd":    {"signal": False, "bullish": False},
            "stoch":   {"signal": False, "bullish": False},
            "bullish": False,   # cả 2 chỉ báo đều bullish
            "ok":      False,   # có ít nhất 1 crossover mới
        }

    macd  = compute_macd(df["close"])
    stoch = compute_stoch(df)

    # Khung bullish = cả MACD và Stoch đang trên đường tín hiệu
    bullish = macd["bullish"] and stoch["bullish"]
    # Có tín hiệu mới = ít nhất 1 chỉ báo vừa crossover
    has_new = macd["crossover"] or stoch["signal"]

    return {
        "label":   label,
        "macd":    macd,
        "stoch":   stoch,
        "bullish": bullish,
        "ok":      bullish,   # dùng để kiểm tra đồng thuận
        "has_new": has_new,
    }


def analyze_ticker_mtf(ticker: str) -> Optional[dict]:
    """
    Phân tích đa khung thời gian cho 1 mã.
    Trả về dict đầy đủ hoặc None nếu không đủ dữ liệu.
    """
    df_daily = fetch_ohlcv(ticker, days=730)
    if df_daily is None or len(df_daily) < 60:
        return None

    df_weekly  = resample_weekly(df_daily)
    df_monthly = resample_monthly(df_daily)

    tf_monthly = analyze_timeframe(df_monthly, "Tháng")
    tf_weekly  = analyze_timeframe(df_weekly,  "Tuần")
    tf_daily   = analyze_timeframe(df_daily,   "Ngày")

    # Đồng thuận = cả 3 khung đều bullish
    all_bullish = tf_monthly["ok"] and tf_weekly["ok"] and tf_daily["ok"]

    # Đếm số khung bullish
    bullish_count = sum([tf_monthly["ok"], tf_weekly["ok"], tf_daily["ok"]])

    # Giá hiện tại
    cur_price = float(df_daily["close"].iloc[-1])
    prev_price = float(df_daily["close"].iloc[-2]) if len(df_daily) > 1 else cur_price
    chg_pct = round((cur_price - prev_price) / prev_price * 100, 2)

    return {
        "ticker":        ticker,
        "price":         round(cur_price),
        "change_pct":    chg_pct,
        "all_bullish":   all_bullish,
        "bullish_count": bullish_count,
        "timeframes": {
            "monthly": tf_monthly,
            "weekly":  tf_weekly,
            "daily":   tf_daily,
        },
        "updated_at": datetime.now().isoformat(),
    }


# ── FULL SCAN ───────────────────────────────────────────────────────────

def run_mtf_scan(tickers: list = None) -> dict:
    """Quét toàn bộ danh sách, trả về kết quả phân loại."""
    if tickers is None:
        tickers = VN100

    log.info(f"MTF Scan bắt đầu: {len(tickers)} mã...")
    results      = []
    all_bullish  = []
    two_bullish  = []
    errors       = []

    for i, ticker in enumerate(tickers):
        try:
            r = analyze_ticker_mtf(ticker)
            if r:
                results.append(r)
                if r["all_bullish"]:
                    all_bullish.append(r)
                elif r["bullish_count"] >= 2:
                    two_bullish.append(r)
        except Exception as e:
            errors.append({"ticker": ticker, "error": str(e)})
            log.warning(f"Lỗi {ticker}: {e}")
        # Delay 3 giây mỗi 15 mã để tránh rate limit (20 req/phút)
        if (i + 1) % 15 == 0:
            log.info(f"Đã quét {i+1}/{len(tickers)} mã — chờ 65 giây tránh rate limit...")
            time.sleep(65)

    # Sắp xếp: nhiều khung bullish + giá tăng trước
    results.sort(key=lambda x: (x["bullish_count"], x["change_pct"]), reverse=True)

    log.info(f"MTF Scan xong: {len(all_bullish)} mã đồng thuận 3 khung, {len(two_bullish)} mã 2 khung")

    return {
        "all":          results,
        "all_bullish":  all_bullish,   # ⭐ 3 khung đồng thuận
        "two_bullish":  two_bullish,   # 🔵 2 khung đồng thuận
        "summary": {
            "total_scanned":  len(tickers),
            "total_results":  len(results),
            "count_3_frames": len(all_bullish),
            "count_2_frames": len(two_bullish),
            "scan_errors":    len(errors),
            "scanned_at":     datetime.now().isoformat(),
        },
        "errors": errors,
    }
