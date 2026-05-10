import asyncio
from bot_cli.commands.doctor import run_doctor_checks
import os
os.environ['PRIVATE_KEY'] = '0x' + 'ab'*32
os.environ['FUNDER_ADDRESS'] = '0x' + 'cd'*20
os.environ['RPC_URL'] = 'https://polygon-rpc.example.com'
os.environ['CHAIN_ID'] = '137'
os.environ['ENABLE_AUTO_REDEEM'] = 'true'
os.environ['LEADER_WALLETS'] = '0x' + 'ef'*20

out = asyncio.run(run_doctor_checks('live'))
print(out['checks'].get('redemption_wallet'))
