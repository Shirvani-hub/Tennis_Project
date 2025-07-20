import os
import fnmatch

parquet_paths = os.getcwd()
pattern = '*.parquet'
for root, dirs, files in os.walk(parquet_paths):
    for filename in fnmatch.filter(files, pattern):
        print(os.path.join(root, filename))
        os.remove(os.path.join(root, filename))
