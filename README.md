# lighter-splash

Watches every perpetual market on [Lighter](https://app.lighter.xyz) and posts a Telegram
alert the moment a coin makes a sharp move — a "splash". One WebSocket subscription covers
all ~230 markets.

```
🟢 $USELESS
Изм.: +14.82%

MAX: 0.0412
MIN: 0.0359

Now last price: $0.0412
Справедл. price: $0.0411

Spot price: $0.0410

⏱️ 6.4 min
🌊 Volume 24h: $1.24m

🕓 18:22:41 UTC+0

🔗 https://app.lighter.xyz/trade/USELESS
```

## How it reads the market

Lighter exposes `market_stats/all` on `wss://mainnet.zklighter.elliot.ai/stream`. Subscribe
once and you get a snapshot of every market, then only the ones that changed. The REST
equivalent is a 322 KB response per poll, so the socket is both cheaper and faster — you see
a move as it prints rather than on your next poll tick.

Each market keeps a sliding window of `(timestamp, price)` points, `WINDOW_MIN` minutes deep.

## What counts as a splash

Three conditions, and the order matters:

1. **Spread across the window ≥ `THRESHOLD_PCT`** — the gap between the window's high and low.
2. **Price is standing at the edge right now.** At the high it's a pump (🟢), at the low a
   dump (🔴). If the move already retraced into the middle of the range, the news is stale and
   nothing is sent.
3. **It isn't a move that was already reported** (see below).

The clock (`⏱️`) counts from whichever extreme came *first*, so it measures the move itself
rather than the width of the window.

## Not flooding the channel

This is the part that took the most iterations, because a naive version is unusable.

While the price sits at its extreme, conditions 1 and 2 stay true on *every* incoming tick —
one move would fire hundreds of messages. So a market arms and disarms:

- It **re-arms** only when the window's spread collapses back below the threshold, i.e. the old
  extreme has aged out and the move is genuinely over.
- A disarmed market can still fire again if the move **reverses** (that's a different move), or
  if it **extends by another `RETRIGGER_PCT`** from the price of the last alert. A coin that
  pumped 12% alerts again at 12% on top of that, not at 12% from where it started.

An earlier version re-armed whenever the price stepped away from the edge. That looked
reasonable and was wrong: a price wobbling around its low re-armed on every tick that ticked
up, then fired the same alert again on the way back down.

Dead markets are filtered by `MIN_VOL_24H`, and `BLACKLIST` drops coins you don't care about.

## Quickstart

```bash
git clone https://github.com/ownape/Lighter-splash
cd Lighter-splash
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
cp .env.example .env    # put in your bot token and channel
./venv/bin/python splash.py
```

No API key, no account, no signing — the stream is public. You only need a Telegram bot token
and a channel to post into. Set `DRY_RUN=1` to print alerts to the log instead of sending them.

To run it as a service, see [`deploy/lighter-splash.service`](deploy/lighter-splash.service).

## Configuration

| Variable | Default | What it does |
|---|---|---|
| `BOT_TOKEN` | — | Telegram bot token |
| `CHANNEL` | — | `@channelname` or a numeric chat id |
| `THRESHOLD_PCT` | `12` | move size that counts as a splash, % |
| `WINDOW_MIN` | `60` | how far back the window looks, minutes |
| `RETRIGGER_PCT` | = threshold | extra move required before the same direction alerts again |
| `MIN_VOL_24H` | `1000` | ignore markets below this 24h volume, $ |
| `BLACKLIST` | empty | comma-separated tickers to skip |
| `TZ_OFFSET` | `0` | hours added to UTC in the timestamp line |
| `DRY_RUN` | `0` | `1` = log alerts instead of sending |

Tighter window plus lower threshold means faster and noisier; the defaults are a starting
point, not a recommendation.

## License

MIT
