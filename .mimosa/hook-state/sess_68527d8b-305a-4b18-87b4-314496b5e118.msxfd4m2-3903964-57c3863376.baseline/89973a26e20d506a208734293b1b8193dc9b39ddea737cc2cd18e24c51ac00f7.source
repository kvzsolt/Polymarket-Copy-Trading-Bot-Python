"""P1-k: PositionKey must preserve user_id casing.

Ethereum/Polymarket wallet ids are EIP-55 checksum-cased and the repos store
them exactly as written (positions_repo never lowercases user_id on write). The
old ``_normalize_id`` lowercased any 0x-prefixed id, user_id included, so a
PositionKey built from a checksum address queried and locked the lowercase form
while the rows were checksum-cased — producing orphan rows the drift check
cannot find (memory: feedback_user_id_casing). condition_id / token_id remain
lowercased: they are case-insensitive hashes the venue returns lowercased.
"""

from domain.position_key import PositionKey

# A real EIP-55 checksum address (mixed case).
CHECKSUM = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"


def test_checksum_user_id_preserved():
    key = PositionKey(user_id=CHECKSUM, condition_id="0xABCDEF", token_id="123456")
    assert key.user_id == CHECKSUM


def test_user_id_whitespace_stripped_without_lowercasing():
    key = PositionKey(user_id=f"  {CHECKSUM} ", condition_id="0xc", token_id="1")
    assert key.user_id == CHECKSUM


def test_condition_and_token_still_lowercased():
    key = PositionKey(user_id=CHECKSUM, condition_id="0xABCDEF", token_id="0xDEADBEEF")
    assert key.condition_id == "0xabcdef"
    assert key.token_id == "0xdeadbeef"


def test_factories_preserve_checksum_user_id():
    row = {"user_id": CHECKSUM, "condition_id": "0xC", "asset": "999", "outcome": "YES"}
    assert PositionKey.from_stored_position(row).user_id == CHECKSUM
    req = {"user_id": CHECKSUM, "condition_id": "0xC", "token_id": "999"}
    assert PositionKey.from_execution_request(req).user_id == CHECKSUM
