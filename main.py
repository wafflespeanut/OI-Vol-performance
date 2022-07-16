from datetime import datetime
import json
import os
import requests
import sys
import time

out_dir = sys.argv[1]
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

WH_URL = os.environ.get("DISCORD_WEBHOOK_URL")


def get_ordered(oi_vol_24h, vol_24h):
    oi_vol_24h.sort(key=lambda x: x[1])
    vol_24h.sort(key=lambda x: x[1], reverse=True)
    return oi_vol_24h, vol_24h


def get_ftx_info():
    oi_vol_24h, vol_24h = [], []
    futures = requests.get("https://ftx.com/api/futures").json()

    for info in futures["result"]:
        asset = info["underlying"]
        if asset.endswith("USDT") or not info["perpetual"] or info["expired"] or info["volume"] == 0:
            continue
        oi_vol_24h.append((asset, round(info["openInterest"] / info["volume"], 2)))
        vol_24h.append((asset, round(info["volumeUsd24h"], 0)))

    return oi_vol_24h, vol_24h


def get_binance_info():
    oi_vol_24h, vol_24h = [], []
    resp = requests.post("https://api.laevitas.ch/graphql", json={
        "query": "{,getAltsDerivsPerpetual:getAltsDerivsPerpetual(,market:\"BINANCE\",change:\"24\")}",
    }).json()

    for info in resp["data"]["getAltsDerivsPerpetual"]:
        asset = info["base_currency"]["value"]
        if info["margin"]["value"] != "usd":
            continue
        oi_vol_24h.append((asset, round(info["open_interest"]["value"] / info["volume24h"]["value"], 2)))
        vol_24h.append((asset, round(info["volume24h"]["value"], 0)))

    return oi_vol_24h, vol_24h


providers = {
    "Binance": get_binance_info,
    "FTX": get_ftx_info,
}


def post_message(cex, data, post_wh):
    max_asset_len, max_oi_len, max_vol_len = 0, 0, 0
    for (oi_a, oi_v, vol_a, vol_v) in data:
        max_asset_len = max(max_asset_len, len(oi_a) + 1)
        max_oi_len = max(max_oi_len, len(oi_v) + 1)
        max_vol_len = max(max_vol_len, len(vol_v) + 1)

    def _post(content, post_wh):
        if not WH_URL:
            print(content)
            return
        if not post_wh:
            return
        try:
            resp = requests.post(WH_URL, json={"content": content})
            if resp.status_code == 204:
                return
            resp = resp.text
        except Exception as err:
            resp = f"{err}"
        print(f"Failed to post to Discord: {resp}")

    text = "```\n"
    for i, (oi_a, oi_v, vol_a, vol_v) in enumerate(data):
        if i == 0:
            text = f"**{cex}:**\n```\n"
        text += f"{oi_a:<{max_asset_len}}{oi_v:<{max_oi_len}}| {vol_a:<{max_asset_len}}{vol_v:<{max_vol_len}}".rstrip() + "\n"
        if (i + 1) % 50 == 0:
            _post(text + "\n```", post_wh)
            text = "```\n"
    if len(text) > 10:
        _post(text + "\n```", post_wh)


def append_cex_data(now, cex, aggr, post_msg):
    ranks = {}
    ranks.setdefault("assets", [])
    current_json = os.path.join(out_dir, f"{now.strftime('%Y-%m-%d')}-{cex.lower()}-rank.json")
    if os.path.exists(current_json):
        with open(current_json, 'r') as fd:
            ranks = json.load(fd)

    data = []
    ep = int(now.timestamp())
    ranks.setdefault("oi", [[]])
    ranks.setdefault("vol", [[]])
    ranks["oi"][0].append(ep)
    ranks["vol"][0].append(ep)

    temp_oi, temp_vol = {}, {}
    for i, ((oi_a, oi_v), (vol_a, vol_v)) in enumerate(zip(*get_ordered(*aggr()))):
        if oi_a not in ranks["assets"]:
            ranks["assets"].append(oi_a)
        if vol_a not in ranks["assets"]:
            ranks["assets"].append(vol_a)
        temp_oi[oi_a] = i + 1
        temp_vol[vol_a] = i + 1
        data.append((oi_a, str(oi_v), vol_a, f"{vol_v:.2e}"))

    for i, asset in enumerate(ranks["assets"]):
        if len(ranks["oi"]) - 1 < i + 1:
            ranks["oi"].append([])
        ranks["oi"][i + 1].append(len(ranks["assets"]) - temp_oi[asset] + 5)  # reverse ranks for plot
        if len(ranks["vol"]) - 1 < i + 1:
            ranks["vol"].append([])
        ranks["vol"][i + 1].append(len(ranks["assets"]) - temp_vol[asset] + 5)

    with open(current_json, 'w') as fd:
        json.dump(ranks, fd)
    post_message(cex, data, post_msg)


prev_hour = datetime.utcnow().hour


while True:
    now = datetime.utcnow()
    current_hour = now.hour
    hour_changed = current_hour != prev_hour
    if hour_changed:
        prev_hour = current_hour
    for cex, aggr in providers.items():
        try:
            append_cex_data(now, cex, aggr, hour_changed)
            time.sleep(1)
        except Exception as err:
            print(f"Error aggregating data: {err}")
    print(f"Dumped OI/Volume data for {now}")
    time.sleep(5 * 60)
