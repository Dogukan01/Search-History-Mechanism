from collections import OrderedDict

class CompanyHistory:
    def __init__(self):
        self.visitors = {}

    def add_query(self, vid, query):
        if vid not in self.visitors:
            self.visitors[vid] = QueryHistoryMRU()

        return self.visitors[vid].add_query(query)

    def get_queries_by_vid(self, vid):
        if vid in self.visitors:
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