"""
アクティブサイトフィルタ

各サイトのRSSを取得し、指定日数以内に記事があるサイトのみを残す。

使い方:
    python scripts/rank_sites.py --feeds path/to/feeds.json --days 3 --dry-run
    python scripts/rank_sites.py --feeds path/to/feeds.json --days 3
"""

import argparse
import io
import json
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import feedparser
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

UA = {"User-Agent": "Mozilla/5.0 (compatible; MatomeAggregator/1.0)"}


def parse_date(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return parsedate_to_datetime(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def check_feed(feed: dict, cutoff: datetime) -> dict:
    """サイトのRSSを取得し、cutoff以降の記事数を返す"""
    try:
        resp = requests.get(feed["url"], headers=UA, timeout=10)
        resp.raise_for_status()
        d = feedparser.parse(resp.content)
        if d.bozo and not d.entries:
            return {"id": feed["id"], "ok": False, "recent": 0, "total": 0, "error": "parse error"}

        recent = 0
        for entry in d.entries:
            pub = parse_date(entry.get("published", "") or entry.get("updated", ""))
            if pub and pub >= cutoff:
                recent += 1

        return {"id": feed["id"], "ok": True, "recent": recent, "total": len(d.entries)}
    except Exception as e:
        return {"id": feed["id"], "ok": False, "recent": 0, "total": 0, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="指定日数以内にアクティブなサイトのみを残す")
    parser.add_argument("--feeds", required=True, help="feeds.json のパス")
    parser.add_argument("--days", type=int, default=3, help="アクティブ判定の日数 (default: 3)")
    parser.add_argument("--out", help="出力先 (省略時は --feeds を上書き)")
    parser.add_argument("--dry-run", action="store_true", help="結果表示のみ、ファイル更新しない")
    args = parser.parse_args()

    with open(args.feeds, encoding="utf-8") as f:
        feeds = json.load(f)

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    print(f"判定基準: {cutoff.strftime('%Y-%m-%d %H:%M')} UTC 以降に記事があるか ({args.days}日間)")
    print(f"対象サイト数: {len(feeds)}")
    print()

    # ── RSS並列チェック ────────────────────────────────────
    print("RSS取得中...")
    feed_by_id = {f["id"]: f for f in feeds}
    results: dict[str, dict] = {}

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(check_feed, feed, cutoff): feed for feed in feeds}
        done = 0
        for future in as_completed(futures):
            done += 1
            r = future.result()
            results[r["id"]] = r
            if done % 50 == 0:
                print(f"  ... {done}/{len(feeds)}")

    # ── 集計 ──────────────────────────────────────────────
    active_ids = {sid for sid, r in results.items() if r["recent"] > 0}
    fetch_ok = {sid for sid, r in results.items() if r["ok"]}
    fetch_fail = {sid for sid, r in results.items() if not r["ok"]}

    print()
    print(f"結果: {len(active_ids)} アクティブ / {len(fetch_ok)} 取得成功 / {len(fetch_fail)} 取得失敗")
    print()

    # ── カテゴリ別サマリ ──────────────────────────────────
    cat_orig = Counter(f.get("category_name", "?") for f in feeds)
    cat_active = Counter(feed_by_id[sid].get("category_name", "?") for sid in active_ids)

    print(f"  {'カテゴリ':<20} {'元':>5} {'active':>7} {'除外':>5}")
    print(f"  {'-' * 45}")
    for cat in sorted(cat_orig, key=lambda c: cat_orig[c], reverse=True):
        o = cat_orig[cat]
        a = cat_active.get(cat, 0)
        print(f"  {cat:<20} {o:>5} {a:>7} {o - a:>5}")
    total_o = len(feeds)
    total_a = len(active_ids)
    print(f"  {'-' * 45}")
    print(f"  {'合計':<20} {total_o:>5} {total_a:>7} {total_o - total_a:>5}")
    print()

    # ── 除外サイト一覧 ────────────────────────────────────
    removed = [f for f in feeds if f["id"] not in active_ids]
    if removed:
        print(f"除外サイト ({len(removed)}件):")
        for f in removed:
            r = results[f["id"]]
            reason = r.get("error", "記事0件") if not r["ok"] else "期間内の記事なし"
            print(f"  - {f['name']}  [{reason}]")
        print()

    # ── 出力 ──────────────────────────────────────────────
    if args.dry_run:
        print("[dry-run] ファイルは更新していません。")
        return

    filtered = [f for f in feeds if f["id"] in active_ids]
    out_path = args.out or args.feeds
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(filtered, f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(filtered)} サイトを {out_path} に保存しました (元: {len(feeds)} サイト)")


if __name__ == "__main__":
    main()
