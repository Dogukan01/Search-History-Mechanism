from typing import Optional
from core.history_database import RedisDB
from core.tcp_server import start_tcp_server
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

class SetRequest(BaseModel):
    key: str
    value: str
    ex: int | None = None

class HSetRequest(BaseModel):
    key: str
    field: str
    value: str

class ZAddRequest(BaseModel):
    key: str
    score: float
    member: str

def nightly_backup_job():
    """Her gece saat 03:00'te tetiklenecek ana cron fonksiyonu"""
    print(f"[CRON JOB] Gece yedekleme işlemi başladı. Saat: {datetime.now()}")
    
    # 1. Adım: O anki tarihi alıp dosya adı üretiyoruz
    today_str = datetime.now().strftime("%Y-%m-%d")
    cron_filename = f"redis_lite_{today_str}.rdb"
    
    # 2. Adım: BGSAVE ile arka planda yedekle ve AOF'yi döndür
    db.bgsave(filename=cron_filename)
    
    # 3. Adım: 7 günden eski olan dosyaları arkada temizle
    db.cleanup_old_backups()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # [AÇILIŞ] Konteyner başlarken diskteki veriyi RAM'e geri yükle
    db.load_from_disk()
    
    # TCP Sunucusunu arka planda başlatıyoruz
    loop = asyncio.get_event_loop()
    tcp_task = loop.create_task(start_tcp_server(db))
    
    # Zamanlayıcıyı (Scheduler) kuruyoruz
    scheduler = BackgroundScheduler()
    
    # Cron kuralı: Her gün (day_of_week='*'), saat 03:00'te (hour=3, minute=0) tetikle
    trigger = CronTrigger(hour=3, minute=0, day_of_week='*')
    scheduler.add_job(nightly_backup_job, trigger=trigger, id="nightly_backup")
    
    # Zamanlayıcıyı arka planda asenkron olarak uykuya yatırıyoruz, saati gelince uyanacak
    scheduler.start()
    print("[SİSTEM] Gece 03:00 CronJob zamanlayıcısı başarıyla başlatıldı.")
    
    yield
    
    # TCP sunucusunu kapatıyoruz
    tcp_task.cancel()
    try:
        await tcp_task
    except asyncio.CancelledError:
        pass

    # [KAPANIŞ] Sunucu kapatılırken veriler kaybolmasın diye son bir yedek alıyoruz
    print("[SİSTEM] Sunucu kapatılıyor, kapanış yedeği alınıyor...")
    db.save_to_disk(filename="redis_lite_dump.rdb")
    scheduler.shutdown()

app = FastAPI(root_path="/redis", lifespan=lifespan)
db = RedisDB()

### Search History'e ait Endpointler

@app.post("/set_history")
def set_history_value(vid: str, query: str, cid: Optional[str] = "default"):
    try:
        result = db.set_history(cid, vid, query)
        return {"status": "OK", "new_lenght": result}
    except TypeError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_history")
def get_history_value(cid: Optional[str] = "default", vid: Optional[str] = None):
    try:
        result = db.get_history(cid, vid)
        return result
    except TypeError as e:
        raise HTTPException(status_code=400, detail=str(e))


### Manuel Yedek Alma Endpointi

@app.post("/admin/backup")
async def trigger_manual_backup():
    """
    Swagger UI üzerinden veya kod içinden manuel tetikleyebileceğin yedekleme endpoint'i.
    API'yi kilitlememesi için işlemi arka plan görevi olarak çalıştırır (BGSAVE).
    """
    # Manuel yedek ismini saat ve dakika içererek üretiyoruz
    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    manual_filename = f"redis_lite_manual_{now_str}.rdb"
    
    # BGSAVE kendi thread'ini başlattığı için BackgroundTasks kullanmaya gerek yok
    db.bgsave(filename=manual_filename)
    
    return {
        "durum": "Yedekleme işlemi arka planda (BGSAVE) başlatıldı",
        "olusturulan_dosya": manual_filename,
        "mesaj": "Verileriniz RDB'ye aktarılıyor ve AOF dosyası yenileniyor."
    }