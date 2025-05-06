import pyarrow.parquet as pq
import pickle

class PQDataAccess():
    def __init__(self, address, batch_size=1):
        self.batch_size = batch_size
        self.iterator = None

        with open (address + '_info', 'rb') as fp:
            self.files_info = pickle.load(fp)
                        
    def get_item(self):
        temp_list = []
        for info in self.files_info:
            table_df = pq.read_table(info['uri']).to_pandas()
            for index, row in table_df.iterrows():
                temp_list.append(row)
                if len(temp_list) == self.batch_size:
                    yield temp_list
                    temp_list=[]

    def get_batch(self):
        if self.iterator == None:
            self.iterator = self.get_item()
        try:
            elem = next(self.iterator)
        except StopIteration:
            self.iterator = self.get_item()
            elem = next(self.iterator)
        return elem
