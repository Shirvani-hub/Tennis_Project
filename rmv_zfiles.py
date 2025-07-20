import os
import fnmatch

zfile_paths = os.getcwd()
pattern = "*.zip"

for root, dirs, files in os.walk(zfile_paths):
    for filename in fnmatch.filter(files, pattern):
        print(os.path.join(root, filename))
        os.remove(os.path.join(root, filename))
