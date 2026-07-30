class CompanySearchHistory:
    def __init__(self, max_visitors=50000):
        # Python 3.7+ standart dict sıralamayı korur, OrderedDict'ten yaklaşık %30-40 daha az bellek harcar
        self.visitors = {}
        self.max_visitors = max_visitors

    def add_query(self, vid, query):
        if vid not in self.visitors:
            self.visitors[vid] = VisitorSearchCache()
        else:
            # Mevcut ziyaretçi işlem yaparsa, aktifliğini (LRU) sona taşıyarak güncelle
            # dict için: silip tekrar eklemek öğeyi en sona (en yeniye) ekler
            cache = self.visitors.pop(vid)
            self.visitors[vid] = cache

        result = self.visitors[vid].add_query(query)
        
        # Kapasite sınırı aşıldıysa, en eski (ilk sıradaki) ziyaretçiyi bellekten temizle
        if len(self.visitors) > self.max_visitors:
            first_key = next(iter(self.visitors))
            del self.visitors[first_key]

        return result

    def get_queries_by_vid(self, vid):
        if vid in self.visitors:
            # Ziyaretçi bilgisi okunduğunda da onu aktif sayıp sona at
            cache = self.visitors.pop(vid)
            self.visitors[vid] = cache
            return cache.get_query()
        return []

    def delete_history(self, vid, query=None):
        if vid not in self.visitors:
            return
        if query is not None:
            self.visitors[vid].delete_query(query)
            if len(self.visitors[vid].value) <= 0:
                del self.visitors[vid]
        else:
            del self.visitors[vid]


class VisitorSearchCache:
    def __init__(self):
        self.value = {}
    
    def add_query(self, query):
        # Yeni sorguyu ekle, varsa sona taşımak için önce sil
        if query in self.value:
            del self.value[query]
        self.value[query] = None
        
        # 20 sınırını aştıysa, en eskiyi (ilk sıradakini) sil
        if len(self.value) > 20:
            first_key = next(iter(self.value))
            del self.value[first_key]
            
        return len(self.value)

    def get_query(self):
        # Yeni eklenenler sonda olduğu için, çıktıyı ters çevirerek (en yeni -> en eski) döndürüyoruz
        return list(self.value.keys())[::-1]

    def delete_query(self, query):
        if query in self.value:
            del self.value[query]