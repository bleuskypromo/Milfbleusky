import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from atproto import Client

CONFIG_PATH = "config.json"
STATE_PATH = "state.json"


# -----------------------
# JSON helpers (robust)
# -----------------------
def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return default
            return json.loads(content)
    except Exception as e:
        print(f"[WARN] Could not parse {path} ({e}). Using defaults.")
        return default


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


# -----------------------
# Time/window helpers
# -----------------------
def now_ts() -> float:
    return time.time()


def iso_to_ts(iso_str: str) -> Optional[float]:
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def ts_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def parse_created_at(post: Dict[str, Any]) -> float:
    s = post.get("indexedAt") or post.get("record", {}).get("createdAt") or post.get("createdAt")
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def within_window(post: Dict[str, Any], start_ts: float, end_ts: float) -> bool:
    created = parse_created_at(post)
    if created <= 0:
        return False
    return start_ts <= created <= end_ts


# -----------------------
# URL-proof config normalization (feeds/lists/single post)
# -----------------------
def _extract_actor_and_rkey(url: str, segment: str) -> Optional[Tuple[str, str]]:
    """
    For:
      https://bsky.app/profile/<actor>/<segment>/<rkey>
    segment: "feed" or "lists"
    """
    u = url.strip()
    lower = u.lower()
    if "bsky.app/profile/" not in lower:
        return None
    try:
        tail = u.split("bsky.app/profile/", 1)[1]
        parts = [p for p in tail.split("/") if p]
        if len(parts) < 3:
            return None
        actor = parts[0]
        seg = parts[1].lower()
        rkey = parts[2]
        if seg != segment:
            return None
        return actor, rkey
    except Exception:
        return None


def resolve_actor_to_did(client: Client, actor: str) -> str:
    a = actor.strip()
    if a.lower().startswith("did:"):
        return a
    res = client.com.atproto.identity.resolve_handle(params={"handle": a})
    return getattr(res, "did", "") or ""


def normalize_feed_uris(client: Client, values: List[str]) -> List[str]:
    """
    Accepts:
      - at://did/app.bsky.feed.generator/rkey
      - https://bsky.app/profile/<actor>/feed/<rkey>
    """
    out: List[str] = []
    for v in values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if not v:
            continue

        if v.startswith("at://"):
            out.append(v)
            continue

        parsed = _extract_actor_and_rkey(v, "feed")
        if parsed:
            actor, rkey = parsed
            did = resolve_actor_to_did(client, actor)
            if did:
                out.append(f"at://{did}/app.bsky.feed.generator/{rkey}")
            continue

        print(f"[WARN] Unrecognized feed value, skipping: {v}")

    return out


def normalize_list_uris(client: Client, values: List[str]) -> List[str]:
    """
    Accepts:
      - at://did/app.bsky.graph.list/rkey
      - https://bsky.app/profile/<actor>/lists/<rkey>
    """
    out: List[str] = []
    for v in values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if not v:
            continue

        if v.startswith("at://"):
            out.append(v)
            continue

        parsed = _extract_actor_and_rkey(v, "lists")
        if parsed:
            actor, rkey = parsed
            did = resolve_actor_to_did(client, actor)
            if did:
                out.append(f"at://{did}/app.bsky.graph.list/{rkey}")
            continue

        print(f"[WARN] Unrecognized list value, skipping: {v}")

    return out


def normalize_post_uri(client: Client, value: str) -> str:
    """
    Accepts:
      - at://did/app.bsky.feed.post/rkey
      - https://bsky.app/profile/<actor>/post/<rkey>
    Returns AT-URI or "" if not recognized.
    """
    if not isinstance(value, str):
        return ""
    v = value.strip()
    if not v:
        return ""

    if v.startswith("at://"):
        return v

    lower = v.lower()
    if "bsky.app/profile/" in lower:
        try:
            tail = v.split("bsky.app/profile/", 1)[1]
            parts = [p for p in tail.split("/") if p]
            # parts: [actor, "post", rkey]
            if len(parts) >= 3 and parts[1].lower() == "post":
                actor = parts[0]
                rkey = parts[2]
                did = resolve_actor_to_did(client, actor)
                if did:
                    return f"at://{did}/app.bsky.feed.post/{rkey}"
        except Exception:
            pass

    print(f"[WARN] Unrecognized single_post_uri, skipping: {value}")
    return ""


# -----------------------
# Blacklist (URL-proof)
# -----------------------
def normalize_blocked_users(values: List[str]) -> set:
    """
    Accepts:
      - did:plc:xxxx
      - handle.bsky.social
      - https://bsky.app/profile/handle
      - https://bsky.app/profile/did:plc:xxxx
    """
    out = set()
    for v in values:
        if not isinstance(v, str):
            continue
        v = v.strip()
        if not v:
            continue
        lower = v.lower()

        if "bsky.app/profile/" in lower:
            try:
                tail = v.split("bsky.app/profile/", 1)[1].strip("/")
                actor = tail.split("/", 1)[0].strip()
                out.add(actor.lower())
                continue
            except Exception:
                pass

        out.add(lower)
    return out


def is_blocked(post: Dict[str, Any], blocked: set) -> bool:
    author = post.get("author") or {}
    did = (author.get("did") or "").lower()
    handle = (author.get("handle") or "").lower()
    return (did in blocked) or (handle in blocked)


# -----------------------
# Filters:
# - Only media (photo/video)
# - No reply
# - No repost (where detectable)
# - Feeds/lists require #milf
# - URL in text is allowed (as long as media exists)
# -----------------------
def is_reply(post: Dict[str, Any]) -> bool:
    record = post.get("record") or {}
    return bool(record.get("reply"))


def embed_is_media_only(post: Dict[str, Any]) -> bool:
    """
    Accept only:
      - app.bsky.embed.images
      - app.bsky.embed.video
      - app.bsky.embed.recordWithMedia (ONLY if its media part is images/video)
    Reject:
      - app.bsky.embed.external (link card)
      - app.bsky.embed.record (quote without media)
      - no embed (text-only)
    """
    embed = post.get("embed")
    if not embed:
        return False

    et = embed.get("$type") or ""

    if "app.bsky.embed.images" in et:
        imgs = embed.get("images") or []
        return len(imgs) > 0

    if "app.bsky.embed.video" in et:
        return True

    if "app.bsky.embed.external" in et:
        return False

    if "app.bsky.embed.record" in et and "recordWithMedia" not in et:
        return False

    if "app.bsky.embed.recordWithMedia" in et:
        media = embed.get("media") or {}
        mt = media.get("$type") or ""
        if "app.bsky.embed.images" in mt:
            imgs = media.get("images") or []
            return len(imgs) > 0
        if "app.bsky.embed.video" in mt:
            return True
        return False

    return False


def is_repost_reason(feed_item: Dict[str, Any]) -> bool:
    """
    Feed/list items may contain:
      reason: {$type: app.bsky.feed.defs#reasonRepost, ...}
    """
    reason = feed_item.get("reason")
    if not reason:
        return False
    rt = reason.get("$type") or ""
    return "app.bsky.feed.defs#reasonRepost" in rt


def has_required_milf_tag(post: Dict[str, Any]) -> bool:
    """
    True if post contains #milf (case-insensitive), either in text or tag facet.
    """
    record = post.get("record") or {}

    text = (record.get("text") or "")
    if re.search(r"(?i)(?:^|\s)#milf\b", text):
        return True

    facets = record.get("facets") or []
    for f in facets:
        feats = f.get("features") or []
        for feat in feats:
            t = feat.get("$type") or ""
            if "app.bsky.richtext.facet#tag" in t:
                tag = (feat.get("tag") or "").lower()
                if tag == "milf":
                    return True

    return False


def passes_content_filters(post: Dict[str, Any], feed_item_context: Optional[Dict[str, Any]]) -> bool:
    # No reposts (detectable for feeds/lists)
    if feed_item_context and is_repost_reason(feed_item_context):
        return False

    # No replies
    if is_reply(post):
        return False

    # Feeds + lists only must have #milf (context exists for feed/list items)
    if feed_item_context and not has_required_milf_tag(post):
        return False

    # Must be media (photo/video); this blocks text-only and link-card-only
    if not embed_is_media_only(post):
        return False

    return True


# -----------------------
# Post helpers
# -----------------------
def get_author_did(post: Dict[str, Any]) -> str:
    author = post.get("author") or {}
    return author.get("did") or ""


def get_author_handle(post: Dict[str, Any]) -> str:
    author = post.get("author") or {}
    return author.get("handle") or ""


def get_uri(post: Dict[str, Any]) -> str:
    return post.get("uri") or ""


def get_cid(post: Dict[str, Any]) -> str:
    return post.get("cid") or ""


# -----------------------
# Fetchers
# -----------------------
def _to_dict(x: Any) -> Dict[str, Any]:
    if hasattr(x, "model_dump"):
        return x.model_dump()
    if isinstance(x, dict):
        return x
    try:
        return dict(x)
    except Exception:
        return {}


def fetch_feed_items(client: Client, feed_uri: str, limit: int) -> List[Dict[str, Any]]:
    res = client.app.bsky.feed.get_feed(params={"feed": feed_uri, "limit": limit})
    items = getattr(res, "feed", None) or []
    return [_to_dict(it) for it in items]


def fetch_list_items(client: Client, list_uri: str, limit: int) -> List[Dict[str, Any]]:
    res = client.app.bsky.feed.get_list_feed(params={"list": list_uri, "limit": limit})
    items = getattr(res, "feed", None) or []
    return [_to_dict(it) for it in items]


def fetch_hashtag_posts(client: Client, tag: str, limit: int) -> List[Dict[str, Any]]:
    q = tag.strip()
    if not q:
        return []
    if not q.startswith("#"):
        q = "#" + q
    res = client.app.bsky.feed.search_posts(params={"q": q, "limit": limit})
    posts = getattr(res, "posts", None) or []
    return [_to_dict(p) for p in posts]


def fetch_single_post(client: Client, uri: str) -> Optional[Dict[str, Any]]:
    try:
        res = client.app.bsky.feed.get_posts(params={"uris": [uri]})
        posts = getattr(res, "posts", None) or []
        if not posts:
            return None
        return _to_dict(posts[0])
    except Exception:
        return None


# -----------------------
# Single post cycle (unrepost -> repost each run)
# -----------------------
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


# -----------------------
# Main
# -----------------------
def main() -> None:
    username = os.environ.get("BSKY_USERNAME", "").strip()
    password = os.environ.get("BSKY_PASSWORD", "").strip()
    if not username or not password:
        raise SystemExit("Missing BSKY_USERNAME / BSKY_PASSWORD env vars")

    config = load_json(CONFIG_PATH, {})

    raw_feeds = norm_list(config.get("feeds", []))
    raw_lists = norm_list(config.get("lists", []))
    hashtags = norm_list(config.get("hashtags", []))
    raw_single_post = (config.get("single_post_uri") or "").strip()

    blocked_users = normalize_blocked_users(config.get("blocked_users", []))

    max_total = int(config.get("max_total_per_run", 100))
    max_per_author = int(config.get("max_per_author_per_run", 3))
    delay_seconds = float(config.get("delay_seconds", 2))

    fetch_limit_feed = int(config.get("fetch_limit_per_feed", 50))
    fetch_limit_list = int(config.get("fetch_limit_per_list", 50))
    search_limit_tag = int(config.get("search_limit_per_tag", 50))

    overlap_minutes = int(config.get("overlap_minutes", 15))
    fallback_hours = int(config.get("fallback_hours_first_run", 3))
    state_max_uris = int(config.get("state_max_uris", 8000))

    state = load_json(
        STATE_PATH,
        {"reposted_uris": [], "single_repost_record_uri": "", "last_run_iso": ""},
    )
    reposted_uris = set(state.get("reposted_uris", []))
    single_repost_record_uri = (state.get("single_repost_record_uri") or "").strip()
    last_run_iso = (state.get("last_run_iso") or "").strip()
    last_run_ts = iso_to_ts(last_run_iso)

    client = Client()
    client.login(username, password)

    # URL-proof normalization (needs login for handle->did resolve)
    feeds = normalize_feed_uris(client, raw_feeds)
    lists = normalize_list_uris(client, raw_lists)
    single_post_uri = normalize_post_uri(client, raw_single_post) if raw_single_post else ""

    end_ts = now_ts()
    if last_run_ts is None:
        start_ts = end_ts - (fallback_hours * 3600)
    else:
        start_ts = last_run_ts - (overlap_minutes * 60)

    print(f"[INFO] Window ALL sources: {ts_to_iso(start_ts)} -> {ts_to_iso(end_ts)}")
    print(f"[INFO] Blocked users: {len(blocked_users)}")
    print(f"[INFO] Feeds: {len(feeds)} | Lists: {len(lists)} | Tags: {len(hashtags)}")

    # 0) Cycle the single post repost (unrepost previous record first)
    if single_repost_record_uri:
        ok, msg = try_unrepost_by_record_uri(client, single_repost_record_uri)
        if ok:
            print(f"[OK]  single unrepost: {single_repost_record_uri}")
            single_repost_record_uri = ""
        else:
            print(f"[WARN] single unrepost failed: {msg}")

    # 1) Collect with context (context exists for feed/list items, None for hashtag results)
    collected: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []

    # Feeds
    for f in feeds:
        try:
            items = fetch_feed_items(client, f, fetch_limit_feed)
            for it in items:
                p = it.get("post")
                if isinstance(p, dict):
                    collected.append((p, it))
        except Exception as e:
            print(f"[WARN] feed fetch failed: {f} :: {e}")

    # Lists
    for l in lists:
        try:
            items = fetch_list_items(client, l, fetch_limit_list)
            for it in items:
                p = it.get("post")
                if isinstance(p, dict):
                    collected.append((p, it))
        except Exception as e:
            print(f"[WARN] list fetch failed: {l} :: {e}")

    # Hashtags (no repost-reason context available)
    for t in hashtags:
        try:
            posts = fetch_hashtag_posts(client, t, search_limit_tag)
            for p in posts:
                if isinstance(p, dict):
                    collected.append((p, None))
        except Exception as e:
            print(f"[WARN] hashtag fetch failed: {t} :: {e}")

    # 2) Apply filters + de-dup by URI
    by_uri: Dict[str, Dict[str, Any]] = {}

    for p, ctx in collected:
        uri = get_uri(p)
        cid = get_cid(p)
        if not uri or not cid:
            continue

        if not within_window(p, start_ts, end_ts):
            continue

        if uri in reposted_uris:
            continue

        if blocked_users and is_blocked(p, blocked_users):
            continue

        if not passes_content_filters(p, ctx):
            continue

        by_uri[uri] = p

    candidates = list(by_uri.values())
    candidates.sort(key=parse_created_at, reverse=True)

    # 3) Enforce per-author cap
    author_count = defaultdict(int)
    limited: List[Dict[str, Any]] = []
    take_limit = max(0, max_total - 1)  # reserve 1 slot for single post

    for p in candidates:
        if len(limited) >= take_limit:
            break
        author = get_author_did(p)
        if author and author_count[author] >= max_per_author:
            continue
        limited.append(p)
        if author:
            author_count[author] += 1

    # 4) Fetch single post + validate + inject at position 3 (index 2)
    final_queue: List[Dict[str, Any]] = limited
    single_post: Optional[Dict[str, Any]] = None

    if single_post_uri:
        single_post = fetch_single_post(client, single_post_uri)
        if not single_post:
            print(f"[WARN] single post not found or fetch failed: {raw_single_post}")

    if single_post:
        # Apply same safety filters to single post (except repost reason, since no ctx)
        if blocked_users and is_blocked(single_post, blocked_users):
            print("[WARN] Single post author is blocked — skipping single post")
            single_post = None
        elif not passes_content_filters(single_post, None):
            print("[WARN] Single post does not meet media/no-reply rules — skipping single post")
            single_post = None

    if single_post and get_uri(single_post) and get_cid(single_post):
        idx = 2
        if idx > len(final_queue):
            idx = len(final_queue)
        final_queue = final_queue[:idx] + [single_post] + final_queue[idx:]
        final_queue = final_queue[:max_total]

    print(f"[INFO] Queue size: {len(final_queue)} (max_total={max_total})")

    # 5) Execute reposts with delay
    newly_reposted: List[str] = []
    for i, p in enumerate(final_queue, start=1):
        uri = get_uri(p)
        cid = get_cid(p)
        if not uri or not cid:
            continue

        is_single = bool(single_post_uri) and (uri == single_post_uri)

        ok, msg, repost_record_uri = try_repost(client, uri, cid)
        if ok:
            author_handle = get_author_handle(p)
            print(f"[OK]  {i:02d} reposted: {uri}  (by {author_handle})")
            if is_single and repost_record_uri:
                single_repost_record_uri = repost_r