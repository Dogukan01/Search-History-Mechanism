from collections import OrderedDict

class CompanyHistory:
    def __init__(self, max_visitors=50000):
        # LRU mantığı kurabilmek için OrderedDict kullanıyoruz
        self.visitors = OrderedDict()
        self.max_visitors = max_visitors

    def add_query(self, vid, query):
        if vid not in self.visitors:
            self.visitors[vid] = QueryHistoryMRU()
        else:
            # Mevcut ziyaretçi işlem yaparsa, aktifliğini (LRU) güncelle
            self.visitors.move_to_end(vid, last=True)

        result = self.visitors[vid].add_query(query)
        
        # Kapasite sınırı aşıldıysa, en eski (ilk sıradaki) ziyaretçiyi bellekten temizle
        if len(self.visitors) > self.max_visitors:
            self.visitors.popitem(last=False)

        return result

    def get_queries_by_vid(self, vid):
        if vid in self.visitors:
            # Ziyaretçi bilgisi okunduğunda da onu aktif sayıp sona at
            self.visitors.move_to_end(vid, last=True)
            return self.visitors[vid].get_query()
        return []

    def get_all(self):
        return {vid: history.get_query() for vid, history in self.visitors.items()}

        
class QueryHistoryMRU:
    def __init__(self):
        self.value = OrderedDict()
    
    def add_query(self, query):
        self.value[query] = None
        self.value.move_to_end(query, last=False)
        if len(self.value) > 20:
            self.value.popitem(last=True)
        return len(self.value)

    def get_query(self):
        return list(self.value.keys())