from typing import Optional
from core.engine import HistoryStore
from fastapi import FastAPI, HTTPException
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

def nightly_backup_job():
    """Her gece saat 03:00'te tetiklenecek ana cron fonksiyonu"""
    print(f"[CRON JOB] Gece yedekleme işlemi başladı. Saat: {datetime.now()}")
    
    # 1. Adım: O anki tarihi alıp dosya adı üretiyoruz
    today_str = datetime.now().strftime("%Y-%m-%d")
    cron_filename = f"search_history_{today_str}.db"
    
    # 2. Adım: BGSAVE ile arka planda yedekle ve AOF'yi döndür
    db.bgsave(filename=cron_filename)
    
    # 3. Adım: 7 günden eski olan dosyaları arkada temizle
    db.cleanup_old_backups()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # [AÇILIŞ] Konteyner başlarken diskteki veriyi RAM'e geri yükle
    db.load_from_disk()
    
    # Zamanlayıcıyı (Scheduler) kuruyoruz
    scheduler = BackgroundScheduler()
    
    # Cron kuralı: Her gün (day_of_week='*'), saat 03:00'te (hour=3, minute=0) tetikle
    trigger = CronTrigger(hour=3, minute=0, day_of_week='*')
    scheduler.add_job(nightly_backup_job, trigger=trigger, id="nightly_backup")
    
    # Zamanlayıcıyı arka planda asenkron olarak uykuya yatırıyoruz, saati gelince uyanacak
    scheduler.start()
    print("[SİSTEM] Gece 03:00 CronJob zamanlayıcısı başarıyla başlatıldı.")
    
    try:
        yield
    except asyncio.CancelledError:
        pass
    finally:
        # [KAPANIŞ] Sunucu kapatılırken veriler kaybolmasın diye son bir yedek alıyoruz
        print("[SİSTEM] Sunucu kapatılıyor, kapanış yedeği alınıyor...")
        db.save_to_disk(filename="search_history_dump.db")
        scheduler.shutdown()

app = FastAPI(root_path="/redis", lifespan=lifespan)
db = HistoryStore()

### Search History'e ait Endpointler

@app.post("/set_history")
def set_history_value(vid: str, query: str, cid: Optional[str] = "default"):
    try:
        result = db.set_history(cid, vid, query)
        return {"status": "OK", "new_lenght": result}
    except TypeError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_history")
def get_history_value(vid: str, cid: Optional[str] = "default"):
    try:
        result = db.get_history(cid, vid)
        return result
    except TypeError as e:
        raise HTTPException(status_code=400, detail=str(e))
