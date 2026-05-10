import asyncio
import os
from bot_cli.commands.doctor import run_doctor_checks

os.environ['PRIVATE_KEY'] = '0x' + 'ab'*32
os.environ['FUNDER_ADDRESS'] = '0x05F18F8C6d90Ce52F88eE2f89a038462C9122B3E'  # random address
os.environ['RPC_URL'] = 'https://polygon-rpc.example.com'  # dead RPC
os.environ['CHAIN_ID'] = '137'
os.environ['ENABLE_AUTO_REDEEM'] = 'true'
os.environ['LEADER_WALLETS'] = '0x' + 'ef'*20

out = asyncio.run(run_doctor_checks('live'))
print(out['checks'].get('redemption_wallet'))
