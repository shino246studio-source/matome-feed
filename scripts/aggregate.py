# scripts/aggregate.py
import feedparser
import json
import os
import re
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from feeds import FEEDS

# サムネイル抽出：RSSの書き方がサイトによって異なるため複数パターンに対応
def extract_thumbnail(entry: dict) -> str | None:
    # パターン1: media:thumbnail / media:content
    media_thumbnail = entry.get("media_thumbnail")
    if media_thumbnail:
        return media_thumbnail[0].get("url")

    media_content = entry.get("media_content")
    if media_content:
        url = media_content[0].get("url", "")
        if url.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")):
            return url

    # パターン2: enclosure（Podcast系だが一部まとめサイトも使用）
    enclosures = entry.get("enclosures", [])
    for enc in enclosures:
        if enc.get("type", "").startswith("image/"):
            return enc.get("href") or enc.get("url")

    # パターン3: summary/content内のimgタグから最初の画像を抽出
    for field in ("summary", "content"):
        text = ""
        val = entry.get(field)
        if isinstance(val, list) and val:
            text = val[0].get("value", "")
        elif isinstance(val, str):
            text = val
        match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', text)
        if match:
            url = match.group(1)
            # 相対URLや1x1トラッキングピクセルを除外
            if url.startswith("http") and not re.search(r'[1-9]x[1-9]\.', url):
                return url

    return None


HATENA_API = "https://bookmark.hatenaapis.com/count/entries"


def fetch_hatena_batch(urls: list[str]) -> dict[str, int]:
    """50件以下のURLバッチに対してはてブ数を取得"""
    try:
        resp = requests.get(HATENA_API, params=[("url", u) for u in urls], timeout=10)
        resp.raise_for_status()
        return resp.json()  # {"url": count, ...}
    except Exception as e:
        print(f"[WARN] Hatena API error: {e}")
        return {}


def fetch_hatena_counts(urls: list[str]) -> dict[str, int]:
    """全URLのはてブ数を50件ずつ並列取得"""
    chunks = [urls[i:i + 50] for i in range(0, len(urls), 50)]
    counts = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_hatena_batch, chunk): chunk for chunk in chunks}
        for future in as_completed(futures):
            counts.update(future.result())

    return counts


def fetch_feed(feed_info: dict) -> dict:
    try:
        # タイムアウト設定必須（GitHub Actionsで詰まり防止）
        resp = requests.get(
            feed_info["url"],
            headers={"User-Agent": "Mozilla/5.0 (compatible; MatomeAggregator/1.0)"},
            timeout=10,
        )
        resp.raise_for_status()
        d = feedparser.parse(resp.content)

        if d.bozo and not d.entries:
            raise ValueError(f"Feed parse error: {d.bozo_exception}")

        articles = []
        for entry in d.entries[:30]:
            published = entry.get("published", "") or entry.get("updated", "")
            articles.append({
                "id":        entry.get("id") or entry.get("link", ""),
                "title":     entry.get("title", "").strip(),
                "url":       entry.get("link", ""),
                "published": published,
                "thumbnail": extract_thumbnail(entry),
                "site_id":   feed_info["id"],
                "site_name": feed_info["name"],
            })

        return {"site_id": feed_info["id"], "articles": articles, "ok": True}

    except Exception as e:
        print(f"[ERROR] {feed_info['id']}: {e}")
        return {"site_id": feed_info["id"], "articles": [], "ok": False, "error": str(e)}


def main():
    os.makedirs("output", exist_ok=True)

    all_articles = []
    site_status = {}

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_feed, f): f for f in FEEDS}
        for future in as_completed(futures):
            result = future.result()
            site_status[result["site_id"]] = result["ok"]
            all_articles.extend(result["articles"])

    # 日付でソート（パースできない日付は末尾に）
    def sort_key(a):
        try:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(a["published"])
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    all_articles.sort(key=sort_key, reverse=True)

    # 上位3000件に絞ってからはてブ数を取得
    all_articles = all_articles[:3000]
    article_urls = [a["url"] for a in all_articles if a["url"]]
    hatena_counts = fetch_hatena_counts(article_urls)
    for article in all_articles:
        article["hatena_bookmarks"] = hatena_counts.get(article["url"], 0)

    print(f"📚 Hatena bookmarks fetched for {len(article_urls)} articles")

    output = {
        "schema_version": 1,
        "updated_at":     datetime.now(timezone.utc).isoformat(),
        "total":          len(all_articles),
        "articles":       all_articles,
        "site_status":    site_status,
    }

    path = "output/feed.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))  # 本番用に圧縮

    print(f"✅ {len(all_articles)} articles → {path}")


if __name__ == "__main__":
    main()