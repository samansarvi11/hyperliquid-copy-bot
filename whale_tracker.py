import asyncio
import json
from collections import Counter, deque
from decimal import Decimal, InvalidOperation

import requests
import websockets


WALLET = "0x8af700ba841f30e0a3fcb0ee4c4a9d223e1efa05".lower()

INFO_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"

MAX_SUBSCRIPTIONS = 1000
SUBSCRIBE_BATCH_SIZE = 50
SUBSCRIBE_DELAY = 0.05

RECENT_TIDS_LIMIT = 10000


def info_request(payload):
    response = requests.post(
        INFO_URL,
        json=payload,
        timeout=20,
        )
    response.raise_for_status()
    return response.json()


def discover_perp_coins():
    """
    Discover all live perpetual markets across the native Hyperliquid
    perp DEX and HIP-3 / builder-deployed perp DEXs.

    Returns:
    coins: {coin_name: dex_name}
    dex_names: list of discovered DEX names
    """

    dex_rows = info_request({"type": "perpDexs"})

    dex_names = [""]

    for row in dex_rows:
        if row and row.get("name"):
            dex_names.append(row["name"])

    coins = {}

    for dex in dex_names:
        payload = {
            "type": "meta",
            "dex": dex,
        }

        meta = info_request(payload)

        for market in meta.get("universe", []):
            coin = market.get("name")

            if not coin:
                continue

            if market.get("isDelisted", False):
                continue

            coins[coin] = dex

    return coins, dex_names


def format_dex_name(dex):
    if not dex:
        return "Hyperliquid"

    return dex


def print_whale_trade(trade, dex):
    """
    Check whether the whale is one of the two trade counterparties.
    Hyperliquid documents users as [buyer, seller].
    """

    users = trade.get("users")

    if not isinstance(users, list):
        return

    if len(users) != 2:
        return

    buyer = str(users[0]).lower()
    seller = str(users[1]).lower()

    if WALLET != buyer and WALLET != seller:
        return

    if WALLET == buyer:
        whale_side = "BUY"
    else:
        whale_side = "SELL"

    coin = trade.get("coin", "?")
    price = str(trade.get("px", "?"))
    size = str(trade.get("sz", "?"))
    trade_time = trade.get("time", "?")
    trade_id = trade.get("tid", "?")
    trade_hash = trade.get("hash", "?")

    try:
        value = Decimal(price) * Decimal(size)
        value_text = f"${value:,.2f}"
    except (InvalidOperation, ValueError):
        value_text = "?"

    print()
    print("=" * 70)
    print("WHALE TRADE DETECTED")
    print("=" * 70)
    print(f"DEX:       {format_dex_name(dex)}")
    print(f"Market:    {coin}")
    print(f"Side:      {whale_side}")
    print(f"Size:      {size}")
    print(f"Price:     {price}")
    print(f"Value:     {value_text}")
    print(f"Time:      {trade_time}")
    print(f"Trade ID:  {trade_id}")
    print(f"Hash:      {trade_hash}")
    print("=" * 70)


async def subscribe_to_all_trades(ws, coins):
    """
    Subscribe to every discovered perpetual market.

    Hyperliquid currently allows up to 1000 websocket subscriptions.
    Our current market count is far below that limit.
    """

    coin_list = sorted(coins.keys())

    total = len(coin_list)

    if total > MAX_SUBSCRIPTIONS:
        raise RuntimeError(
            f"Found {total} markets, exceeding the configured "
            f"subscription limit of {MAX_SUBSCRIPTIONS}."
            )

    print(f"Sending {total} trade subscriptions...")

    successful_requests = 0

    for index, coin in enumerate(coin_list, start=1):

        message = {
        "method": "subscribe",
        "subscription": {
            "type": "trades",
            "coin": coin,
        },
        }

        await ws.send(json.dumps(message))

        successful_requests += 1

        if index % SUBSCRIBE_BATCH_SIZE == 0:
            print(f"Subscription requests sent: {index}/{total}")
            await asyncio.sleep(SUBSCRIBE_DELAY)

    print(
        f"All {successful_requests} trade subscription "
        f"requests were sent."
    )


def remember_tid(recent_tids, tid):
    """
    Return True if tid is new.
    Return False if it was already processed.
    """

    if tid is None:
        return True

    if tid in recent_tids:
        return False

    recent_tids.append(tid)

    return True


def print_subscription_error(message):
    data = message.get("data")

    print()
    print("WEBSOCKET SUBSCRIPTION ERROR")
    print(f"Data: {data}")
    print()


def process_message(message, coins, recent_tids):
    """
    Process one decoded WebSocket message.
    """

    channel = message.get("channel")

    if channel == "subscriptionResponse":
        return

    if channel == "error":
        print_subscription_error(message)
        return

    if channel != "trades":
        return

    data = message.get("data")

    if not isinstance(data, list):
        return

    for trade in data:

        if not isinstance(trade, dict):
            continue

        tid = trade.get("tid")

        if not remember_tid(recent_tids, tid):
            continue

        coin = trade.get("coin")

        if not coin:
            continue

        dex = coins.get(coin, "?")

        print_whale_trade(
            trade,
            dex,
        )


async def monitor_once():
    """
    One complete WebSocket monitoring session.

    If Hyperliquid disconnects us, the caller will reconnect.
    """

    print()
    print("=" * 70)
    print("DISCOVERING HYPERLIQUID PERPETUAL MARKETS")
    print("=" * 70)

    coins, dex_names = discover_perp_coins()

    print(f"Discovered DEX entries: {len(dex_names)}")
    print(f"Discovered live markets: {len(coins)}")

    dex_counter = Counter(
        dex if dex else "Hyperliquid"
        for dex in coins.values()
        )

    print()
    print("Markets by DEX:")

    for dex, count in sorted(dex_counter.items()):
        print(f"  {dex}: {count}")

    print()
    print("Connecting to Hyperliquid WebSocket...")
    print(f"URL: {WS_URL}")

    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_size=None,
    ) as ws:

        print("WebSocket connected.")

        await subscribe_to_all_trades(
            ws,
            coins,
        )

        print()
        print("=" * 70)
        print("READ-ONLY WHALE TRACKER IS RUNNING")
        print("=" * 70)
        print(f"Whale: {WALLET}")
        print(f"Markets monitored: {len(coins)}")
        print("Waiting for a trade involving the whale...")
        print("Press Ctrl+C to stop.")
        print("=" * 70)

        recent_tids = deque(
            maxlen=RECENT_TIDS_LIMIT
        )

        async for raw_message in ws:

            try:
                message = json.loads(raw_message)

            except json.JSONDecodeError:
                continue

            process_message(
                message,
                coins,
                recent_tids,
            )


async def main():

    reconnect_delay = 5

    while True:

        try:
            await monitor_once()

        except KeyboardInterrupt:
            print()
            print("Tracker stopped.")
            break

        except Exception as error:
            print()
            print("=" * 70)
            print("WEBSOCKET / TRACKER ERROR")
            print("=" * 70)
            print(type(error).__name__)
            print(str(error))
            print("=" * 70)
            print(
                f"Reconnecting in {reconnect_delay} seconds..."
                )

            await asyncio.sleep(reconnect_delay)


if __name__ == "__main__":
    asyncio.run(main())
