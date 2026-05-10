import asyncio
import os
os.environ['SIGNATURE_TYPE'] = '2'
from utils.createClobClient import detect_wallet_signature_type
from config.env import WalletConfig, SignatureType

async def main():
    cfg = WalletConfig(private_key='0x'+'ab'*32, chain_id=137, signature_type=SignatureType.EOA, funder_address='0x'+'cd'*20, polygon_rpc_url='http://fail')
    res = await detect_wallet_signature_type(cfg)
    print(res)

asyncio.run(main())
