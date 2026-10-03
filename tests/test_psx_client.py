import httpx
import pytest

from kmi30.config import SourceConfig
from kmi30.psx_client import PKT_OFFSET_S, PSXClient, PSXError, PSXSchemaError, parse_indices_html

from .conftest import pkt

MON_10 = int(pkt(2026, 9, 14, 10, 0).timestamp())  # true UTC epoch for Monday 10:00 PKT


def client_with(handler, calendar, **cfg):
    return PSXClient(SourceConfig(**cfg), calendar, transport=httpx.MockTransport(handler))


def series(rows):
    return lambda req: httpx.Response(200, json={"status": 1, "message": "", "data": rows})


async def test_intraday_wallclock_newest_first_is_sorted_and_shifted(calendar):
    utc = [MON_10 + 60 * i for i in range(30)]
    rows = [[t + PKT_OFFSET_S, 250_000 + i, 100] for i, t in enumerate(utc)][::-1]
    c = client_with(series(rows), calendar)
    ticks = await c.intraday()
    assert [t.ts for t in ticks] == utc
    assert ticks[0].value == 250_000 and ticks[-1].value == 250_029
    assert c.timestamp_mode == "pkt_wallclock"
    await c.aclose()


async def test_intraday_detects_true_utc_encoding(calendar):
    rows = [[MON_10 + 60 * i, 250_000.0, 1] for i in range(30)][::-1]
    c = client_with(series(rows), calendar)
    ticks = await c.intraday()
    assert ticks[0].ts == MON_10
    assert c.timestamp_mode == "utc"
    await c.aclose()


async def test_forced_timestamp_mode(calendar):
    rows = [[MON_10, 1.0, 1]]
    c = client_with(series(rows), calendar, timestamp_mode="pkt_wallclock")
    assert (await c.intraday())[0].ts == MON_10 - PKT_OFFSET_S
    await c.aclose()


async def test_status_zero_is_error(calendar):
    c = client_with(lambda r: httpx.Response(200, json={"status": 0, "message": "bad symbol", "data": []}), calendar)
    with pytest.raises(PSXError, match="bad symbol"):
        await c.intraday()
    await c.aclose()


@pytest.mark.parametrize("resp", [
    httpx.Response(200, text="<html>maintenance</html>"),
    httpx.Response(200, json={"status": 1}),
    httpx.Response(200, json={"status": 1, "data": [["x", "y"]]}),
    httpx.Response(200, json={"status": 1, "data": [[1]]}),
])
async def test_schema_breaks_raise_schema_error(calendar, resp):
    c = client_with(lambda r: resp, calendar)
    with pytest.raises(PSXSchemaError):
        await c.intraday()
    await c.aclose()


async def test_http_error_and_transport_error(calendar):
    c = client_with(lambda r: httpx.Response(503), calendar)
    with pytest.raises(PSXError, match="503"):
        await c.intraday()
    await c.aclose()

    def boom(r):
        raise httpx.ConnectTimeout("hang")

    c = client_with(boom, calendar)
    with pytest.raises(PSXError):
        await c.intraday()
    await c.aclose()


async def test_eod_field_order(calendar):
    day = MON_10 + PKT_OFFSET_S
    rows = [[day, 251_000.0, 1.2e8, 250_500.0], [day - 86400, 250_000.0, 1.1e8, 249_000.0]]
    c = client_with(series(rows), calendar)
    eod = await c.eod()
    assert [r.close for r in eod] == [250_000.0, 251_000.0]
    assert eod[-1].open == 250_500.0 and eod[-1].volume == 1.2e8
    await c.aclose()


INDICES_HTML = """
<table class="tbl"><thead><tr><th>Index</th><th>High</th><th>Low</th><th>Current</th>
<th>Change</th><th>% Change</th></tr></thead><tbody>
<tr><td><a href="/indices/KSE100">KSE100</a></td><td>1</td><td>1</td><td>170,511.85</td><td>10</td><td>0.1%</td></tr>
<tr><td><a href="/indices/KMI30">KMI30</a></td><td data-order="262000.5">262,000.50</td>
<td data-order="259000">259,000.00</td><td data-order="261234.56">261,234.56</td>
<td data-order="-1234.44">-1,234.44</td><td>-0.47%</td></tr></tbody></table>
"""


def test_parse_indices_with_headers_and_data_order():
    snap = parse_indices_html(INDICES_HTML, "KMI30")
    assert snap.current == 261_234.56
    assert snap.high == 262_000.5 and snap.low == 259_000
    assert snap.change == -1234.44 and snap.change_pct == -0.47
    assert snap.prev_close == pytest.approx(262_469.0)


def test_parse_indices_without_headers_and_spaced_symbol():
    html = "<table><tr><td>KMI-30</td><td>2</td><td>1</td><td>1,500.25</td><td>5</td><td>0.3</td></tr></table>"
    assert parse_indices_html(html, "KMI30").current == 1500.25


def test_parse_indices_missing_row():
    with pytest.raises(PSXSchemaError):
        parse_indices_html("<table><tr><td>KSE100</td><td>1</td><td>1</td><td>1</td></tr></table>", "KMI30")
