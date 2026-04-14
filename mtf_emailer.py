"""
mtf_emailer.py — Gửi email bảng tổng hợp MTF hàng ngày qua SendGrid.
"""

import os
import logging
from datetime import datetime
import httpx

log = logging.getLogger(__name__)

SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
ALERT_EMAIL_TO   = os.environ.get("ALERT_EMAIL_TO", "")
ALERT_EMAIL_FROM = os.environ.get("ALERT_EMAIL_FROM", "")

# ── Icon / màu helper ───────────────────────────────────────────────────

def tf_cell(tf: dict) -> str:
    """Render ô trạng thái cho 1 khung thời gian."""
    if not tf:
        return _cell("—", "#444", "#222")

    macd  = tf.get("macd", {})
    stoch = tf.get("stoch", {})
    ok    = tf.get("ok", False)

    macd_icon  = "↑" if macd.get("crossover") else ("▲" if macd.get("bullish") else "▼")
    stoch_icon = "↑" if stoch.get("signal")   else ("▲" if stoch.get("bullish") else "▼")

    macd_color  = "#00e5a0" if macd.get("bullish")  else "#ff4757"
    stoch_color = "#00e5a0" if stoch.get("bullish") else "#ff4757"

    bg  = "rgba(0,229,160,0.08)"  if ok else "rgba(255,71,87,0.06)"
    bdr = "#00e5a044"              if ok else "#ff475744"

    k_val = stoch.get("k", "—")
    d_val = stoch.get("d", "—")
    hist  = macd.get("hist", 0) or 0

    return f"""<td style="padding:10px 8px;border:1px solid #222;background:{bg};
                          border-left:3px solid {'#00e5a0' if ok else '#ff4757'};">
      <div style="font-size:13px;font-weight:700;color:{'#00e5a0' if ok else '#ff4757'};">
        {'✅' if ok else '❌'}
      </div>
      <div style="font-size:10px;color:{macd_color};margin-top:4px;">
        MACD {macd_icon} {'+' if hist>0 else ''}{hist:.3f}
      </div>
      <div style="font-size:10px;color:{stoch_color};">
        Stoch {stoch_icon} K:{k_val} D:{d_val}
      </div>
    </td>"""


def _cell(text, color, bg):
    return f'<td style="padding:10px 8px;border:1px solid #222;background:{bg};color:{color};font-size:12px;">{text}</td>'


# ── Build HTML ──────────────────────────────────────────────────────────

def build_mtf_email(scan_result: dict) -> str:
    all_bullish = scan_result.get("all_bullish", [])
    two_bullish = scan_result.get("two_bullish", [])
    summary     = scan_result.get("summary", {})
    now_str     = datetime.now().strftime("%d/%m/%Y %H:%M")

    def stock_rows(stocks, label, label_color):
        if not stocks:
            return f"""<tr><td colspan="6" style="padding:12px;text-align:center;
                        color:#636b7a;font-size:12px;border:1px solid #222;">
                        Không có mã nào — {label}</td></tr>"""
        rows = ""
        for s in stocks:
            tf = s.get("timeframes", {})
            chg = s.get("change_pct", 0)
            chg_color = "#00e5a0" if chg >= 0 else "#ff4757"
            chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"

            rows += f"""<tr>
              <td style="padding:10px 12px;border:1px solid #222;border-left:3px solid {label_color};">
                <div style="font-family:'Courier New',monospace;font-size:16px;font-weight:800;color:#e8eaf0;">
                  {s['ticker']}
                </div>
                <div style="font-size:10px;color:#636b7a;margin-top:2px;">
                  {s.get('price',0):,.0f} đ &nbsp;
                  <span style="color:{chg_color};">{chg_str}</span>
                </div>
              </td>
              {tf_cell(tf.get('monthly'))}
              {tf_cell(tf.get('weekly'))}
              {tf_cell(tf.get('daily'))}
              <td style="padding:10px 8px;border:1px solid #222;text-align:center;">
                <span style="background:{label_color}22;color:{label_color};
                             border:1px solid {label_color}44;border-radius:20px;
                             padding:3px 10px;font-size:11px;font-weight:700;">
                  {s['bullish_count']}/3
                </span>
              </td>
            </tr>"""
        return rows

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0a0c0f;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<div style="max-width:680px;margin:0 auto;padding:20px;">

  <!-- Header -->
  <div style="background:#111418;border-radius:16px;padding:20px 24px;margin-bottom:16px;
              border:1px solid rgba(255,255,255,0.07);">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <div>
        <span style="font-size:22px;font-weight:800;color:#e8eaf0;">
          VN<span style="color:#00e5a0;">Scan</span>
          <span style="font-size:13px;font-weight:400;color:#636b7a;margin-left:8px;">
            Multi-Timeframe
          </span>
        </span>
        <div style="font-size:11px;color:#636b7a;margin-top:4px;">
          MACD(12,26,9) + Stochastic(8,5,3) — Tháng / Tuần / Ngày
        </div>
      </div>
      <div style="text-align:right;">
        <div style="font-size:11px;color:#636b7a;">Cập nhật</div>
        <div style="font-size:13px;font-weight:600;color:#00e5a0;">{now_str}</div>
      </div>
    </div>
  </div>

  <!-- Summary -->
  <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:16px;">
    <div style="background:#111418;border:1px solid rgba(0,229,160,0.2);border-radius:10px;padding:12px;text-align:center;">
      <div style="font-size:28px;font-weight:800;color:#00e5a0;">{summary.get('count_3_frames',0)}</div>
      <div style="font-size:10px;color:#636b7a;margin-top:2px;">⭐ 3 Khung đồng thuận</div>
    </div>
    <div style="background:#111418;border:1px solid rgba(0,149,255,0.2);border-radius:10px;padding:12px;text-align:center;">
      <div style="font-size:28px;font-weight:800;color:#0095ff;">{summary.get('count_2_frames',0)}</div>
      <div style="font-size:10px;color:#636b7a;margin-top:2px;">🔵 2 Khung đồng thuận</div>
    </div>
    <div style="background:#111418;border:1px solid rgba(255,255,255,0.07);border-radius:10px;padding:12px;text-align:center;">
      <div style="font-size:28px;font-weight:800;color:#636b7a;">{summary.get('total_scanned',0)}</div>
      <div style="font-size:10px;color:#636b7a;margin-top:2px;">Tổng mã quét</div>
    </div>
  </div>

  <!-- Bảng 3 khung đồng thuận -->
  <div style="margin-bottom:16px;">
    <div style="font-size:14px;font-weight:700;color:#00e5a0;margin-bottom:8px;">
      ⭐ 3 Khung đồng thuận — Tín hiệu mạnh nhất
    </div>
    <table style="width:100%;border-collapse:collapse;font-family:monospace;">
      <thead>
        <tr style="background:#1a1f2a;">
          <th style="padding:10px 12px;border:1px solid #222;text-align:left;
                     font-size:11px;color:#636b7a;font-weight:600;">Mã</th>
          <th style="padding:10px 8px;border:1px solid #222;font-size:11px;
                     color:#636b7a;font-weight:600;">📅 Tháng</th>
          <th style="padding:10px 8px;border:1px solid #222;font-size:11px;
                     color:#636b7a;font-weight:600;">📆 Tuần</th>
          <th style="padding:10px 8px;border:1px solid #222;font-size:11px;
                     color:#636b7a;font-weight:600;">📊 Ngày</th>
          <th style="padding:10px 8px;border:1px solid #222;font-size:11px;
                     color:#636b7a;font-weight:600;">Điểm</th>
        </tr>
      </thead>
      <tbody>{stock_rows(all_bullish, "3 khung", "#00e5a0")}</tbody>
    </table>
  </div>

  <!-- Bảng 2 khung đồng thuận -->
  <div style="margin-bottom:16px;">
    <div style="font-size:14px;font-weight:700;color:#0095ff;margin-bottom:8px;">
      🔵 2 Khung đồng thuận — Theo dõi thêm
    </div>
    <table style="width:100%;border-collapse:collapse;font-family:monospace;">
      <thead>
        <tr style="background:#1a1f2a;">
          <th style="padding:10px 12px;border:1px solid #222;text-align:left;
                     font-size:11px;color:#636b7a;font-weight:600;">Mã</th>
          <th style="padding:10px 8px;border:1px solid #222;font-size:11px;
                     color:#636b7a;font-weight:600;">📅 Tháng</th>
          <th style="padding:10px 8px;border:1px solid #222;font-size:11px;
                     color:#636b7a;font-weight:600;">📆 Tuần</th>
          <th style="padding:10px 8px;border:1px solid #222;font-size:11px;
                     color:#636b7a;font-weight:600;">📊 Ngày</th>
          <th style="padding:10px 8px;border:1px solid #222;font-size:11px;
                     color:#636b7a;font-weight:600;">Điểm</th>
        </tr>
      </thead>
      <tbody>{stock_rows(two_bullish[:15], "2 khung", "#0095ff")}</tbody>
    </table>
  </div>

  <!-- Chú thích -->
  <div style="background:#111418;border:1px solid rgba(255,255,255,0.07);
              border-radius:10px;padding:14px 16px;margin-bottom:16px;">
    <div style="font-size:11px;color:#636b7a;line-height:1.8;">
      <b style="color:#e8eaf0;">Chú thích:</b><br>
      ✅ = MACD bullish + Stoch bullish &nbsp;|&nbsp;
      ❌ = Chưa đồng thuận<br>
      ↑ = Vừa crossover phiên này &nbsp;|&nbsp;
      ▲ = Đang bullish (chưa cross mới) &nbsp;|&nbsp;
      ▼ = Bearish<br>
      <b style="color:#00e5a0;">MACD(12,26,9)</b> — EMA crossover &nbsp;|&nbsp;
      <b style="color:#22d3ee;">Stoch(8,5,3)</b> — %K cắt %D dưới 80
    </div>
  </div>

  <!-- CTA -->
  <div style="text-align:center;">
    <a href="https://tuananhstock.onrender.com/app"
       style="display:inline-block;background:#00e5a0;color:#000;border-radius:8px;
              padding:12px 28px;font-weight:700;text-decoration:none;font-size:13px;">
      Mở VNScan →
    </a>
    <div style="font-size:10px;color:#636b7a;margin-top:12px;">
      Báo cáo tự động hàng ngày lúc 16:00. Đây là công cụ hỗ trợ, không phải tư vấn đầu tư.
    </div>
  </div>

</div>
</body></html>"""


# ── Send ────────────────────────────────────────────────────────────────

def send_mtf_email(scan_result: dict) -> bool:
    if not SENDGRID_API_KEY or not ALERT_EMAIL_TO:
        log.warning("Chưa cấu hình SendGrid — bỏ qua gửi email")
        return False

    summary = scan_result.get("summary", {})
    count3  = summary.get("count_3_frames", 0)
    count2  = summary.get("count_2_frames", 0)

    if count3 == 0 and count2 == 0:
        log.info("Không có mã đồng thuận — không gửi email MTF")
        return False

    date_str = datetime.now().strftime("%d/%m/%Y")
    subject  = f"📊 MTF {date_str} — {count3} mã ⭐3 khung | {count2} mã 🔵2 khung | VNScan"

    try:
        payload = {
            "personalizations": [{"to": [{"email": ALERT_EMAIL_TO}], "subject": subject}],
            "from": {"email": ALERT_EMAIL_FROM, "name": "VNScan MTF"},
            "content": [{"type": "text/html", "value": build_mtf_email(scan_result)}],
        }
        r = httpx.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {SENDGRID_API_KEY}",
                     "Content-Type": "application/json"},
            json=payload, timeout=15,
        )
        if r.status_code == 202:
            log.info(f"✅ Email MTF gửi xong — {count3} mã 3 khung, {count2} mã 2 khung")
            return True
        else:
            log.error(f"❌ SendGrid lỗi {r.status_code}: {r.text}")
            return False
    except Exception as e:
        log.error(f"❌ Lỗi gửi email MTF: {e}")
        return False
