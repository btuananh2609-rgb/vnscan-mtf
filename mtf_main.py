"""
mtf_main.py — FastAPI server cho MTF Scanner
- Quét tự động lúc 16:00 mỗi ngày sau khi thị trường đóng
- Phục vụ bảng MTF qua web app
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import asyncio, logging, os
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from mtf_scanner import run_mtf_scan
from mtf_emailer import send_mtf_email

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="VNScan MTF API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_credentials=False, allow_methods=["*"],
                   allow_headers=["*"])

scheduler = AsyncIOScheduler(timezone="Asia/Ho_Chi_Minh")

# ── Cache ──────────────────────────────────────────────────────────────
_cache = {}


@app.on_event("startup")
async def startup():
    log.info("🚀 VNScan MTF khởi động")
    # Delay 10 giây để server bind port trước, rồi mới bắt đầu quét
    async def delayed_scan():
        await asyncio.sleep(10)
        await scheduled_mtf_scan()
    asyncio.create_task(delayed_scan())
    # Quét hàng ngày lúc 16:00 (sau khi thị trường đóng cửa 14:45)
    scheduler.add_job(scheduled_mtf_scan, "cron",
                      day_of_week="mon-fri", hour=16, minute=0)
    # Quét thêm buổi sáng 8:30 để cập nhật trạng thái đầu ngày
    scheduler.add_job(scheduled_mtf_scan, "cron",
                      day_of_week="mon-fri", hour=8, minute=30)
    scheduler.start()


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


async def scheduled_mtf_scan():
    log.info("⏱ MTF Scan bắt đầu...")
    try:
        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_mtf_scan)
        _cache["mtf_result"]  = result
        _cache["last_scan"]   = datetime.now().isoformat()
        log.info(f"✅ MTF xong: {result['summary']['count_3_frames']} mã 3 khung")
        # Gửi email nếu có mã đồng thuận
        await loop.run_in_executor(None, send_mtf_email, result)
    except Exception as e:
        log.error(f"❌ MTF lỗi: {e}")


# ── API Endpoints ───────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "VNScan MTF", "status": "running",
            "last_scan": _cache.get("last_scan")}


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


@app.get("/api/mtf")
def get_mtf(filter: str = "all"):
    result = _cache.get("mtf_result")
    if not result:
        return {"error": "Chưa có dữ liệu — đang quét lần đầu, thử lại sau 2 phút"}

    # Luôn trả về đủ 2 danh sách để frontend render cả 2 bảng
    return {
        "stocks":       result.get("all", []),
        "all_bullish":  result.get("all_bullish", []),
        "two_bullish":  result.get("two_bullish", []),
        "summary":      result.get("summary", {}),
        "last_scan":    _cache.get("last_scan"),
        "filter":       filter,
    }


@app.post("/api/mtf/trigger")
async def trigger_scan():
    asyncio.create_task(scheduled_mtf_scan())
    return {"message": "MTF scan đang chạy, gọi lại /api/mtf sau 2 phút"}


@app.get("/app", response_class=HTMLResponse)
def serve_app():
    html_path = os.path.join(os.path.dirname(__file__), "mtf-dashboard.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h2>File mtf-dashboard.html chưa được upload</h2>")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("mtf_main:app", host="0.0.0.0", port=port)
