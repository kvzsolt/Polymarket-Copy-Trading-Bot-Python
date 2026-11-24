"""Bounded public inventory reads cannot manufacture absence or precision."""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from pmcopy.integrations.polymarket.data_api_positions import DataApiPositionSizeReader

WALLET = "0x" + "1" * 40


@pytest.mark.asyncio
async def test_reads_past_first_full_page_and_includes_dust() -> None:
    first = [{"asset": str(index), "size": "1"} for index in range(1, 501)]
    http = AsyncMock()
    http.get_json.side_effect = [first, [{"asset": "999", "size": "0.125"}]]
    reader = DataApiPositionSizeReader(lambda: http)
    assert await reader.shares(WALLET, "999") == Decimal("0.125")
    calls = http.get_json.await_args_list
    assert len(calls) == 2
    assert all("offset" not in call.kwargs["params"] for call in calls)
    assert all(call.kwargs["params"]["sizeThreshold"] == 0 for call in calls)
    assert all(call.kwargs["params"]["limit"] == 500 for call in calls)


@pytest.mark.asyncio
async def test_v2_envelope_and_snake_case_fields_with_cursor_pagination() -> None:
    first_page = {
        "data": [{"token_id": str(index), "current_size": "1"} for index in range(1, 501)],
        "pagination": {"limit": 500, "offset": 0, "has_more": True, "next_cursor": "cursor-page-2"},
    }
    second_page = {
        "data": [{"token_id": "999", "current_size": "0.125"}],
        "pagination": {"limit": 500, "offset": 0, "has_more": False, "next_cursor": None},
    }
    http = AsyncMock()
    http.get_json.side_effect = [first_page, second_page]
    reader = DataApiPositionSizeReader(lambda: http)
    assert await reader.shares(WALLET, "999") == Decimal("0.125")
    calls = http.get_json.await_args_list
    assert len(calls) == 2
    assert "cursor" not in calls[0].kwargs["params"]
    assert calls[1].kwargs["params"]["cursor"] == "cursor-page-2"


@pytest.mark.asyncio
async def test_zero_requires_completed_validated_inventory() -> None:
    http = AsyncMock()
    http.get_json.return_value = []
    assert await DataApiPositionSizeReader(lambda: http).shares(WALLET, "999") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("broken_page", [None, {}, [None], [{"asset": "1", "size": "2"}]])
async def test_bad_or_repeated_page_cannot_confirm_absence(broken_page) -> None:
    http = AsyncMock()
    http.get_json.side_effect = [
        [{"asset": str(index), "size": "1"} for index in range(1, 501)],
        broken_page,
    ]
    assert await DataApiPositionSizeReader(lambda: http).shares(WALLET, "999") is None


@pytest.mark.asyncio
async def test_exhausted_pagination_cannot_confirm_absence() -> None:
    http = AsyncMock()
    http.get_json.side_effect = [
        [{"asset": str(offset + index), "size": "1"} for index in range(1, 501)]
        for offset in range(0, 10500, 500)
    ]
    assert await DataApiPositionSizeReader(lambda: http).shares(WALLET, "999999") is None
    assert http.get_json.await_count == 21


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [True, False, 0.25, "NaN", "Infinity", "-0.1", None, [], {}])
async def test_invalid_size_is_unknown_even_for_other_token(size) -> None:
    http = AsyncMock()
    http.get_json.return_value = [{"asset": "1", "size": size}]
    assert await DataApiPositionSizeReader(lambda: http).shares(WALLET, "999") is None


@pytest.mark.asyncio
async def test_exact_json_decoder_is_passed_to_transport() -> None:
    http = AsyncMock()
    http.get_json.return_value = []
    await DataApiPositionSizeReader(lambda: http).shares(WALLET, "999")
    decoder = http.get_json.await_args.kwargs["json_decoder"]
    assert decoder('[{"asset":"999","size":0.1234567890123456789012345678}]') == [
        {"asset": "999", "size": Decimal("0.1234567890123456789012345678")}
    ]


@pytest.mark.asyncio
async def test_exception_is_unknown_without_secret_in_logs(caplog) -> None:
    http = AsyncMock()
    marker = "SYNTHETIC_PRIVATE_DSN_MARKER"
    http.get_json.side_effect = ValueError(marker)
    assert await DataApiPositionSizeReader(lambda: http).shares(WALLET, "999") is None
    assert marker not in caplog.text


@pytest.mark.asyncio
async def test_cancellation_propagates() -> None:
    http = AsyncMock()
    http.get_json.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await DataApiPositionSizeReader(lambda: http).shares(WALLET, "999")


@pytest.mark.asyncio
@pytest.mark.parametrize("wallet,token", [("bad", "1"), (WALLET, "0"), (WALLET, "bad")])
async def test_invalid_identity_never_requests_data(wallet, token) -> None:
    http = AsyncMock()
    assert await DataApiPositionSizeReader(lambda: http).shares(wallet, token) is None
    http.get_json.assert_not_awaited()
