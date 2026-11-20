#!/usr/bin/env python3
"""Deterministic fake Polygon/Polymarket endpoint for the CI docker smoke.

The application's startup fail-closes until ``polymarket.rpc_url`` names an
approved provider endpoint (``polygon-mainnet.g.alchemy.com`` /
``polygon-mainnet.infura.io`` — see ``ProviderEndpoints``), and the paper-mode
``/health`` contract additionally requires real ``api_ok`` balance evidence
through that endpoint. CI has no provider credential, so the docker-smoke job
runs this stdlib-only TLS stub on the smoke network, maps the approved
hostname onto it with ``--add-host``, and trusts its throwaway CA by
extending the image's certifi bundle (the provider transport deliberately
ignores ``SSL_CERT_FILE`` because it runs with ``trust_env=False``).

Serves:
* ``POST /`` JSON-RPC for every read method in
  ``ProviderHttpTransport._READ_METHODS`` with plausible exact values
  (chain id 137, non-empty code, uint256 balances).
* ``GET /positions`` — the data-api account-holdings page. An empty list is a
  valid complete page (fewer rows than the page size ends pagination), so
  startup inventory converges on an empty account.

Never answers with real market data; never accepts a write method. The
serving certificate is generated per-run by the CI job and covers only the
smoke hostnames.
"""

import json
import ssl
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CHAIN_ID = "0x89"  # Polygon mainnet 137
BLOCK = hex(50_000_000)
CODE = "0x6080604052"
BALANCE_WEI = "0x1bc16d674ec80000"  # 2 POL
USDC_MICROS = 1_000_000_000  # 1000.000000 USDC
FULL32 = "0x" + "00" * 32

READ_RESULTS = {
    "eth_chainId": CHAIN_ID,
    "net_version": "137",
    "eth_blockNumber": BLOCK,
    "eth_gasPrice": "0x3b9aca00",
    "eth_getCode": CODE,
    "eth_getBalance": BALANCE_WEI,
    "eth_call": "0x" + format(USDC_MICROS, "064x"),
    "eth_getTransactionCount": "0x0",
    "eth_estimateGas": "0x5208",
    "eth_getStorageAt": FULL32,
    "eth_getLogs": [],
    "eth_getTransactionByHash": None,
    "eth_getTransactionReceipt": None,
    "eth_getBlockByHash": "0x" + "ab" * 32,
    "eth_getBlockByNumber": "0x" + "ab" * 32,
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args: object) -> None:  # silence request logging
        pass

    def _send(self, payload: dict[str, object] | list[object], *, status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        # data-api.polymarket.com/positions: an empty account is a valid,
        # complete page (fewer rows than the page size ends pagination).
        if self.path.startswith("/positions"):
            self._send([])
            return
        self._send({"ok": True})

    def do_POST(self) -> None:  # noqa: N802 (http.server API)
        length = int(self.headers.get("Content-Length", 0) or 0)
        try:
            request = json.loads(self.rfile.read(length) or b"{}")
            method = request.get("method")
        except Exception:
            self._send(
                {"jsonrpc": "2.0", "id": 1, "error": {"code": -32700, "message": "parse error"}}
            )
            return
        if not isinstance(method, str) or method not in READ_RESULTS:
            # Read-only stub: unknown/write methods are refused, never faked.
            self._send(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32601, "message": "method not found"},
                }
            )
            return
        self._send({"jsonrpc": "2.0", "id": request.get("id", 1), "result": READ_RESULTS[method]})


def main() -> None:
    cert, key = sys.argv[1], sys.argv[2]
    server = ThreadingHTTPServer(("0.0.0.0", 443), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(cert, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    print("fake polygon rpc ready on :443", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
