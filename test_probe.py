import asyncio
from utils.createClobClient import _fetch_contract_code_hex
print(_fetch_contract_code_hex('https://polygon-rpc.example.com', '0x' + 'cd'*20))
