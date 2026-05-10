import asyncio
from config.app_config import load_config
import os
os.environ['PRIVATE_KEY'] = '0x' + 'ab'*32
os.environ['FUNDER_ADDRESS'] = '0x' + 'cd'*20
os.environ['RPC_URL'] = 'https://polygon-rpc.example.com'
os.environ['CHAIN_ID'] = '137'
os.environ['ENABLE_AUTO_REDEEM'] = 'true'
os.environ['LEADER_WALLETS'] = '0x' + 'ef'*20
from utils.createClobClient import detect_wallet_signature_type
async def main():
    config = load_config()
    print("CONFIG:", config.wallet)
    res = await detect_wallet_signature_type(config.wallet)
    print("DETECTED:", res)
asyncio.run(main())
