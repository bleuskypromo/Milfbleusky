import json
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from atproto import Client

CONFIG_PATH = "config.json"
STATE_PATH = "state.json"


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def norm_list(values: List[str]) -> List[str]:
    out: List[str] = []
    for v in values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if v:
            out.append(v)
    return out


def parse_created_at(post: Dict[str, Any]) -> float:
    s = post.get("indexedAt") or post.get("record", {}).get("createdAt") or post.get("createdAt")
    if not s:
        return 0.0
    try:
        s2 = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s2).timestamp()
    except Exception:
        return 0.0


def get_author_did(post: Dict[str, Any]) -> str:
    author = post.get("author") or {}
    return author.get("did") or ""


def get_uri(post: Dict[str, Any]) -> str:
    return post.get("uri") or ""


def get_cid(post: Dict[str, Any]) -> str:
    return post.get("cid") or ""


def fetch_feed_posts(client: Client, feed_uri: str, limit: int) -> List[Dict[str, Any]]:
    res = client.app.bsky.feed.get_feed(params={"feed": feed_uri, "limit": limit})
    items = getattr(res, "feed", None) or []
    posts: List[Dict[str, Any]] = []
    for it in items:
        p = getattr(it, "post", None)
        if p:
            posts.append(p)
    return posts


def fetch_list_posts(client: Client, list_uri: str, limit: int) -> List[Dict[str, Any]]:
    res = client.app.bsky.feed.get_list_feed(params={"list": list_uri, "limit": limit})
    items = getattr(res, "feed", None) or []
    posts: List[Dict[str, Any]] = []
    for it in items:
        p = getattr(it, "post", None)
        if p:
            posts.append(p)
    return posts


def fetch_hashtag_posts(client: Client, tag: str, limit: int) -> List[Dict[str, Any]]:
    q = tag.strip()
    if not q:
        return []
    if not q.startswith("#"):
        q = "#" + q
    res = client.app.bsky.feed.search_posts(params={"q": q, "limit": limit})
    posts = getattr(res, "posts", None) or []
    return list(posts)


def fetch_single_post(client: Client, uri: str) -> Optional[Dict[str, Any]]:
    try:
        res = client.app.bsky.feed.get_posts(params={"uris": [uri]})
        posts = getattr(res, "posts", None) or []
        return posts[0] if posts else None
    except Exception:
        return None


def extract_rkey_from_at_uri(at_uri: str) -> str:
    try:
        return at_uri.rsplit("/", 1)[-1]
    except Exception:
        return ""


def try_unrepost_by_record_uri(client: Client, repost_record_uri: str) -> Tuple[bool, str]:
    if not repost_record_uri:
        return False, "no repost record uri stored"
    rkey = extract_rkey_from_at_uri(repost_record_uri)
    if not rkey:
        return False, "invalid repost record uri"
    try:
        client.app.bsky.feed.repost.delete(repo=client.me.did, rkey=rkey)
        return True, "unreposted"
    except Exception as e:
        return False, str(e)


def try_repost(client: Client, uri: str, cid: str) -> Tuple[bool, str, str]:
    """
    Returns (success, message, repost_record_uri).
    repost_record_uri is only present on success.
    """
    try:
        res = client.app.bsky.feed.repost.create(
            repo=client.me.did,
            record={
                "subject": {"uri": uri, "cid": cid},
                "createdAt": datetime.utcnow().isoformat() + "Z",
            },
        )
        return True, "reposted", getattr(res, "uri", "") or ""
    except Exception as e:
        return False, str(e), ""


def main() -> None:
    username = os.environ.get("BSKY_USERNAME", "").strip()
    password = os.environ.get("BSKY_PASSWORD", "").strip()
    if not username or not password:
        raise SystemExit("Missing BSKY_USERNAME / BSKY_PASSWORD env vars")

    config = load_json(CONFIG_PATH, {})
    feeds = norm_list(config.get("feeds", []))
    lists = norm_list(config.get("lists", []))
    hashtags = norm_list(config.get("hashtags", []))
    single_post_uri = (config.get("single_post_uri") or "").strip()

    max_total = int(config.get("max_total_per_run", 100))
    max_per_author = int(config.get("max_per_author_per_run", 3))
    delay_seconds = float(config.get("delay_seconds", 2))

    fetch_limit_feed = int(config.get("fetch_limit_per_feed", 50))
    fetch_limit_list = int(config.get("fetch_limit_per_list", 50))
    search_limit_tag = int(config.get("search_limit_per_tag", 50))

    state_max_uris = int(config.get("state_max_uris", 8000))

    state = load_json(STATE_PATH, {"reposted_uris": [], "single_repost_record_uri": ""})
    reposted_uris = set(state.get("reposted_uris", []))
    single_repost_record_uri = (state.get("single_repost_record_uri") or "").strip()

    client = Client()
    client.login(username, password)

    # 0) Always unrepost the previous run's single repost record first (if any)
    if single_repost_record_uri:
        ok, msg = try_unrepost_by_record_uri(client, single_repost_record_uri)
        if ok:
            print(f"[OK]  single unrepost: {single_repost_record_uri}")
            single_repost_record_uri = ""
        else:
            print(f"[WARN] single unrepost failed: {msg}")
            # still continue; we'll attempt repost again later

    # 1) Collect posts: feeds + lists first, then hashtags
    collected: List[Dict[str, Any]] = []

    for f in feeds:
        try:
            collected.extend(fetch_feed_posts(client, f, fetch_limit_feed))
        except Exception as e:
            print(f"[WARN] feed fetch failed: {f} :: {e}")

    for l in lists:
        try:
            collected.extend(fetch_list_posts(client, l, fetch_limit_list))
        except Exception as e:
            print(f"[WARN] list fetch failed: {l} :: {e}")

    for t in hashtags:
        try:
            collected.extend(fetch_hashtag_posts(client, t, search_limit_tag))
        except Exception as e:
            print(f"[WARN] hashtag fetch failed: {t} :: {e}")

    # 2) De-dup by URI, filter already reposted
    by_uri: Dict[str, Dict[str, Any]] = {}
    for p in collected:
        uri = get_uri(p)
        cid = get_cid(p)
        if not uri or not cid:
            continue
        if uri in reposted_uris:
            continue
        by_uri[uri] = p

    candidates = list(by_uri.values())

    # 3) Sort newest first
    candidates.sort(key=parse_created_at, reverse=True)

    # 4) Enforce per-author cap (3 per user per run)
    author_count = defaultdict(int)
    limited: List[Dict[str, Any]] = []

    # Reserve 1 slot for the single post (included in max_total),
    # so take at most max_total - 1 candidates.
    take_limit = max(0, max_total - 1)

    for p in candidates:
        if len(limited) >= take_limit:
            break
        author = get_author_did(p)
        if author and author_count[author] >= max_per_author:
            continue
        limited.append(p)
        if author:
            author_count[author] += 1

    # 5) Fetch & inject single post at position 3 (index 2)
    final_queue: List[Dict[str, Any]] = limited

    single_post = None
    if single_post_uri:
        single_post = fetch_single_post(client, single_post_uri)
        if not single_post:
            print(f"[WARN] single post not found or fetch failed: {single_post_uri}")

    if single_post and get_uri(single_post) and get_cid(single_post):
        idx = 2
        if idx > len(final_queue):
            idx = len(final_queue)
        final_queue = final_queue[:idx] + [single_post] + final_queue[idx:]
        final_queue = final_queue[:max_total]

    print(f"[INFO] Queue size: {len(final_queue)} (max_total={max_total})")

    # 6) Execute reposts with delay
    newly_reposted: List[str] = []
    for i, p in enumerate(final_queue, start=1):
        uri = get_uri(p)
        cid = get_cid(p)
        if not uri or not cid:
            continue

        is_single = bool(single_post_uri) and (uri == single_post_uri)

        ok, msg, repost_record_uri = try_repost(client, uri, cid)
        if ok:
            print(f"[OK]  {i:02d} reposted: {uri}")
            if is_single and repost_record_uri:
                single_repost_record_uri = repost_record_uri
            if not is_single:
                newly_reposted.append(uri)
        else:
            print(f"[SKIP] {i:02d} {uri} :: {msg}")

        time.sleep(delay_seconds)

    # 7) Update state (don’t store the single post itself in reposted_uris)
    for u in newly_reposted:
        reposted_uris.add(u)

    reposted_list = list(reposted_uris)
    if len(reposted_list) > state_max_uris:
        reposted_list = reposted_list[-state_max_uris:]

    save_json(
        STATE_PATH,
        {
            "reposted_uris": reposted_list,
            "single_repost_record_uri": single_repost_record_uri,
        },
    )
    print(f"[INFO] State saved. Total tracked: {len(reposted_list)}")


if __name__ == "__main__":
    main()
