import argparse
import sys

from bot_cli.commands.doctor import register_doctor_command, run_doctor

def main() -> int:
    parser = argparse.ArgumentParser(description="Polymarket Copy Trader Bot CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    register_doctor_command(subparsers)
    
    args = parser.parse_args()
    
    if args.command == "doctor":
        return run_doctor(args)
    
    return 0
