import os
from config.runtime_env import load_runtime_env
from config.env import rehydrate_env
load_runtime_env()
rehydrate_env()
print("SIGNATURE_TYPE:", os.getenv('SIGNATURE_TYPE'))
