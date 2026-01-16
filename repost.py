import os
import re
import json
import time
from typing import Set, Dict, Optional, List, Tuple
from datetime import datetime, timezone, timedelta

from atproto import Client, models

CONFIG_PATH = "config.json"
STATE_PATH = "state.json"
REPOSTED_PATH = "reposted.txt"


# ----------------------------
# Simple state helpers
# ----------------------------
def load_json(path: str, default: dict) -> dict:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            s = f.read().strip()
            if not s:
                return default
            return json.loads(s)
    except Exception as e:
        print(f"[WARN] Could not parse {path}: {e} (using defaults)")
        return default


def save_json(path: str, data: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_reposted() -> Set[str]:
    if not os.path.exists(REPOSTED_PATH):
        return set()
    with open(REPOSTED_PATH, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def save_reposted(rep: Set[str]) -> None:
    with open(REPOSTED_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(rep)))


# ----------------------------
# Time helpers
# ----------------------------
def parse_time(t: Optional[str]) -> Optional[datetime]:
    if not t:
        return None
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ----------------------------
# URL-proof feed normalization
# ----------------------------
def resolve_actor_to_did(c: Client, actor: str) -> str:
    actor = actor.strip()
    if actor.lower().startswith("did:"):
        return actor
    r = c.com.atproto.identity.resolve_handle(
        models.ComAtprotoIdentityResolveHandle.Params(handle=actor)
    )
    return r.did


def normalize_feed_uri(c: Client, value: str) -> str:
    """
    Accepts:
      - at://did/app.bsky.feed.generator/rkey
      - https://bsky.app/profile/<actor>/feed/<rkey>
    Returns at://... or "" if invalid.
    """
    v = (value or "").strip()
    if not v:
        return ""
    if v.startswith("at://"):
        return v

    low = v.lower()
    if "bsky.app/profile/" in low and "/feed/" in low:
        tail = v.split("bsky.app/profile/", 1)[1]
        parts = [p for p in tail.split("/") if p]  # [actor, feed, rkey]
        if len(parts) >= 3 and parts[1].lower() == "feed":
            actor, rkey = parts[0], parts[2]
            did = resolve_actor_to_did(c, actor)
            return f"at://{did}/app.bsky.feed.generator/{rkey}"

    print(f"[WARN] Unrecognized feed URL, skipping: {value}")
    return ""


# ----------------------------
# Content filters
# ----------------------------
def is_reply_record(record) -> bool:
    return getattr(record, "reply", None) is not None


def is_repost_item(item) -> bool:
    # If there's a "reason" in the feed item, treat as repost and skip
    return getattr(item, "reason", None) is not None


def media_ok(record) -> bool:
    """
    Only allow:
      - images
      - video-like embeds
    Reject:
      - text-only (no embed)
      - link-card-only (external)
      - quote/record embeds (record / recordWithMedia)
    """
    e = getattr(record, "embed", None)
    if not e:
        return False

    et = getattr(e, "$type", "") or ""

    # Link card (external) -> reject
    if "app.bsky.embed.external" in et:
        return False

    # Quote / record embeds -> reject (keeps it simple & avoids link/quote reposts)
    # recordWithMedia may contain media but also references another record; treat as not allowed per your "simpel" goal.
    if "app.bsky.embed.record" in et or "app.bsky.embed.recordWithMedia" in et:
        return False

    # Images embed
    imgs = getattr(e, "images", None)
    if imgs:
        try:
            return len(imgs) > 0
        except Exception:
            return True

    # Some libs expose video/media attributes
    if hasattr(e, "video") or hasattr(e, "media"):
        return True

    # Unknown embed -> reject (safe)
    return False


def post_time(item) -> datetime:
    dt = parse_time(getattr(item.post, "indexed_at", None))
    return dt or now_utc()


# ----------------------------
# Actions
# ----------------------------
def do_repost(c: Client, uri: str, cid: str) -> bool:
    try:
        c.repost(uri=uri, cid=cid)
        return True
    except Exception as e:
        print(f"[WARN] repost failed: {e}")
        return False


# ----------------------------
# Main
# ----------------------------
def main():
    username = os.getenv("BSKY_USERNAME", "").strip()
    password = os.getenv("BSKY_PASSWORD", "").strip()
    if not username or not password:
        print("[ERROR] Missing BSKY_USERNAME / BSKY_PASSWORD")
        return

    cfg = load_json(CONFIG_PATH, {})
    feed_input = (cfg.get("feed") or "").strip()

    max_total = int(cfg.get("max_total_per_run", 100))
    max_per_user = int(cfg.get("max_per_author_per_run", 5))
    delay_seconds = float(cfg.get("delay_seconds", 2))

    fetch_limit = int(cfg.get("fetch_limit", 200))  # how many feed items to look at
    overlap_minutes = int(cfg.get("overlap_minutes", 15))
    fallback_hours = int(cfg.get("fallback_hours_first_run", 3))

    state = load_json(STATE_PATH, {"last_run_iso": ""})
    last_run_dt = parse_time(state.get("last_run_iso") or "")

    print("[INFO] Starting bot...")

    c = Client()
    for attempt in range(1, 4):
        try:
            print(f"[INFO] Logging in... (attempt {attempt}/3)")
            c.login(username, password)
            print("[INFO] Login OK")
            break
        except Exception as e:
            print(f"[WARN] Login failed: {e}")
            if attempt == 3:
                print("[ERROR] Giving up on login.")
                return
            time.sleep(5)

    feed_uri = normalize_feed_uri(c, feed_input)
    if not feed_uri:
        print("[ERROR] No valid feed configured in config.json (field: feed)")
        return

    end_dt = now_utc()
    if last_run_dt:
        start_dt = last_run_dt - timedelta(minutes=overlap_minutes)
    else:
        start_dt = end_dt - timedelta(hours=fallback_hours)

    print(f"[INFO] Window: {start_dt.isoformat()} -> {end_dt.isoformat()}")
    print(f"[INFO] Feed: {feed_uri}")

    reposted = load_reposted()
    per_user: Dict[str, int] = {}
    candidates: List[Tuple[datetime, str, str, str]] = []  # (dt, uri, cid, author_did)

    # Fetch feed
    try:
        r = c.app.bsky.feed.get_feed(
            models.AppBskyFeedGetFeed.Params(feed=feed_uri, limit=fetch_limit)
        )
    except Exception as e:
        print(f"[ERROR] get_feed failed: {e}")
        return

    # Collect candidates
    for item in r.feed:
        if is_repost_item(item):
            continue

        post = item.post
        rec = getattr(post, "record", None)
        if not post or not rec:
            continue

        if is_reply_record(rec):
            continue

        if not media_ok(rec):
            continue

        dt = post_time(item)
        if dt < start_dt or dt > end_dt:
            continue

        uri = post.uri
        cid = post.cid
        author_did = post.author.did

        if uri in reposted:
            continue

        candidates.append((dt, uri, cid, author_did))

    # Newest first
    candidates.sort(key=lambda x: x[0], reverse=True)

    # Repost with caps
    done = 0
    for dt, uri, cid, author_did in candidates:
        if done >= max_total:
            break

        if per_user.get(author_did, 0) >= max_per_user:
            continue

        ok = do_repost(c, uri, cid)
        if ok:
            done += 1
            reposted.add(uri)
            per_user[author_did] = per_user.get(author_did, 0) + 1
            print(f"[OK] reposted {uri}")
        else:
            print(f"[SKIP] failed {uri}")

        time.sleep(delay_seconds)

    # Save state
    save_reposted(reposted)
    save_json(STATE_PATH, {"last_run_iso": end_dt.isoformat().replace("+00:00", "Z")})

    print(f"✔ Run klaar | reposts={done} | tracked={len(reposted)}")


if __name__ == "__main__":
    main()