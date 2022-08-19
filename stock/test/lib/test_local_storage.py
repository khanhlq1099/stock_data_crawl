
import os
import shutil

import stock.lib.local_storage as local_storage
from stock.config import ROOT_DIR

class TestFunctionCreateNestedDirectory:
    def test_first(self):
        file_path = os.path.join(ROOT_DIR, "data/raw/cafef/daily_price/2019/PVI/PVI_01.csv")
        dir_path = os.path.dirname(file_path)
        local_storage.create_nested_directory(dir_path)
        
        assert os.path.exists(dir_path)

        if os.path.exists(dir_path):
            os.rmdir(dir_path)
        
