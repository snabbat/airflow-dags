import sys
import os
from context_manager.JsonAccessorManager import JsonAccessorManager

path = os.path.normpath(os.path.join(sys.path[0], "..", "..", "..", "config"))
accessor_manager = JsonAccessorManager()
accessor_manager.add_from_directory(path)
