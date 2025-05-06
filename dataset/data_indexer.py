from pathlib import Path
import pyarrow.parquet as pq
from jsonargparse import CLI
import os, time
import pickle
import os
import time
import pickle
from pathlib import Path
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_file(uri):
    table = pq.read_table(uri)
    table_df = table.to_pandas()
    return {'uri': uri, 'num_rows': len(table_df)}, len(table_df)

def create_index(path: str, output_file: str):
    table_name = path.rsplit('/', 1)[-1]
    print(f'INDEXING DATASET {table_name}\t STARTED')
    st = time.time()
    files_info_list = []
    files = os.listdir(path)
    files = [k for k in files if len(k) == 12 and k.startswith('0000')]
    files = [str(Path(path) / f) for f in files]
    files.sort()
    cnt = 0
    
    if not Path(output_file).is_file():
        with ThreadPoolExecutor(max_workers=128) as executor:
            futures = {executor.submit(process_file, uri): uri for uri in files}
            for future in as_completed(futures):
                file_info, num_rows = future.result()
                files_info_list.append(file_info)
                cnt += num_rows
        
        files_info_list.sort(key=lambda x: x['uri'])
        
        if not Path(output_file).is_file():
            with open(output_file, 'wb') as fp:
                pickle.dump(files_info_list, fp)
                
    end_t = time.time()
    print(f'INDEXING DATASET {table_name}\t FINISHED\t time = {end_t-st}s\t num rows read = {cnt}')
          
def index_dataset(dataset_folder:str ="/home/aac/ml/g3/datasets/data_stfc/export_pqt_4_amd_sftc/") :           
    folders = [dataset_folder + x for x in os.listdir(dataset_folder)]
    folders = [k for k in folders if os.path.isdir(k) ]
    print(folders)
    for folder in folders:
        info_file = folder+'_info'
        create_index(folder, info_file)
    
if __name__ == "__main__":
    CLI(index_dataset)    
