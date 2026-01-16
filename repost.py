import os
import re
import json
import time
from typing import Set, Dict, Optional, List, Tuple
from datetime import datetime, timezone, timedelta

from atproto import Client, models

# ---------------- CONFIG FILES ---------------- #

CONFIG_PATH = "config.json"

REPOSTED_FILE = "reposted_milf.txt"
STATE_FILE = "state_milf.json"  # only stores last_run_iso


# ---------------- BASIC HELPERS ---------------- #

def load_json(path: str, default: dict) -> dict:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            s = f.read().strip()
            if not s:
                return default
            return json.loads(s)
    except Exception:
        return default


def save_json(path: str, data: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_reposted() -> Set[str]:
    if not os.path.exists(REPOSTED_FILE):
        return set()
    with open(REPOSTED_FILE, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def save_reposted(rep: Set[str]) -> None:
    with open(REPOSTED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(rep)))


def parse_time(t: Optional[str]) -> Optional[datetime]:
    if not t:
        return None
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(t)
    except Exception:
        return None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------- URL-PROOF NORMALIZERS ---------------- #

def resolve_actor_to_did(c: Client, actor: str) -> str:
    actor = actor.strip()
    if actor.lower().startswith("did:"):
        return actor
    r = c.com.atproto.identity.resolve_handle(
        models.ComAtprotoIdentityResolveHandle.Params(handle=actor)
    )
    return r.did


def normalize_feed_uri(c: Client, v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    if v.startswith("at://"):
        return v
    # https://bsky.app/profile/<actor>/feed/<rkey>
    if "bsky.app/profile/" in v.lower() and "/feed/" in v.lower():
        tail = v.split("bsky.app/profile/", 1)[1]
        parts = [p for p in tail.split("/") if p]
        # [actor, feed, rkey]
        if len(parts) >= 3 and parts[1].lower() == "feed":
            actor, rkey = parts[0], parts[2]
            did = resolve_actor_to_did(c, actor)
            return f"at://{did}/app.bsky.feed.generator/{rkey}"
    return ""


def normalize_list_uri(c: Client, v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    if v.startswith("at://"):
        return v
    # https://bsky.app/profile/<actor>/lists/<rkey>
    if "bsky.app/profile/" in v.lower() and "/lists/" in v.lower():
        tail = v.split("bsky.app/profile/", 1)[1]
        parts = [p for p in tail.split("/") if p]
        # [actor, lists, rkey]
        if len(parts) >= 3 and parts[1].lower() == "lists":
            actor, rkey = parts[0], parts[2]
            did = resolve_actor_to_did(c, actor)
            return f"at://{did}/app.bsky.graph.list/{rkey}"
    return ""


def normalize_post_uri(c: Client, v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    if v.startswith("at://"):
        return v
    # https://bsky.app/profile/<actor>/post/<rkey>
    if "bsky.app/profile/" in v.lower() and "/post/" in v.lower():
        tail = v.split("bsky.app/profile/", 1)[1]
        parts = [p for p in tail.split("/") if p]
        # [actor, post, rkey]
        if len(parts) >= 3 and parts[1].lower() == "post":
            actor, rkey = parts[0], parts[2]
            did = resolve_actor_to_did(c, actor)
            return f"at://{did}/app.bsky.feed.post/{rkey}"
    return ""


def normalize_blocked_users(values: List[str]) -> set:
    out = set()
    for v in values or []:
        if not isinstance(v, str):
            continue
        s = v.strip().lower()
        if not s:
            continue
        if "bsky.app/profile/" in s:
            try:
                tail = v.split("bsky.app/profile/", 1)[1].strip("/")
                actor = tail.split("/", 1)[0].strip().lower()
                out.add(actor)
                continue
            except Exception:
                pass
        out.add(s)
    return out


def is_blocked(author_did: str, author_handle: str, blocked: set) -> bool:
    return (author_did.lower() in blocked) or (author_handle.lower() in blocked)


# ---------------- FILTERS (Femdom-style) ---------------- #

def media_ok(record) -> bool:
    e = getattr(record, "embed", None)
    if not e:
        return False
    # geen quotes / record embeds
    if hasattr(e, "record"):
        return False
    # images
    if getattr(e, "images", None):
        return True
    # video-ish
    if hasattr(e, "video") or hasattr(e, "media"):
        return True
    return False


def is_reply_record(record) -> bool:
    return getattr(record, "reply", None) is not None


def has_milf_tag_in_text(text: str) -> bool:
    # only require for feeds + lists
    if not text:
        return False
    return re.search(r"(?i)(?:^|\s)#milf\b", text) is not None


def feed_item_is_repost(item) -> bool:
    # Femdom-style: any reason means repost; keep it strict
    return getattr(item, "reason", None) is not None


def get_item_time(item) -> datetime:
    # indexed_at exists on models
    dt = parse_time(getattr(item.post, "indexed_at", None))
    return dt or now_utc()


def valid_common(item) -> bool:
    post = item.post
    rec = getattr(post, "record", None)
    if not post or not rec:
        return False
    if is_reply_record(rec):
        return False
    return media_ok(rec)


# ---------------- LIST MEMBERS (Femdom-style) ---------------- #

def get_list_members(c: Client, uri: str) -> List[str]:
    dids: List[str] = []
    cursor = None
    while True:
        r = c.app.bsky.graph.get_list(
            models.AppBskyGraphGetList.Params(list=uri, limit=100, cursor=cursor)
        )
        for it in r.items:
            subj = it.subject
            did = subj if isinstance(subj, str) else getattr(subj, "did", None)
            if did:
                dids.append(did)
        if not r.cursor:
            break
        cursor = r.cursor
    return dids


# ---------------- SINGLE POST CYCLE (Femdom cleanup-style) ---------------- #

def delete_existing_repost_of_subject(c: Client, my_actor: str, subject_uri: str, max_scan: int = 200) -> int:
    """
    Scan my author feed for repost records that reference subject_uri.
    Delete them so we can repost again.
    """
    deleted = 0
    cursor = None
    scanned = 0

    while True:
        r = c.app.bsky.feed.get_author_feed(
            models.AppBskyFeedGetAuthorFeed.Params(actor=my_actor, limit=100, cursor=cursor)
        )
        if not r.feed:
            break

        for f in r.feed:
            scanned += 1
            post = f.post
            rec = getattr(post, "record", None)
            if not post or not rec:
                continue

            # repost record?
            if getattr(rec, "$type", "") != "app.bsky.feed.repost":
                continue

            subj = getattr(rec, "subject", None)
            subj_uri = getattr(subj, "uri", None) if subj else None
            if subj_uri != subject_uri:
                continue

            try:
                # post.uri is the repost record URI (like Femdom cleanup)
                c.delete_repost(post.uri)
                deleted += 1
            except Exception:
                pass

        if scanned >= max_scan:
            break
        if not r.cursor:
            break
        cursor = r.cursor

    return deleted


def fetch_post_by_uri(c: Client, at_uri: str):
    r = c.app.bsky.feed.get_posts(models.AppBskyFeedGetPosts.Params(uris=[at_uri]))
    if not r.posts:
        return None
    return r.posts[0]


# ---------------- REPOST ACTION (Femdom-style) ---------------- #

def do_repost_like(c: Client, uri: str, cid: str) -> bool:
    try:
        c.repost(uri=uri, cid=cid)
        try:
            c.like(uri=uri, cid=cid)
        except Exception:
            pass
        return True
    except Exception:
        return False


# ---------------- MAIN LOGIC ---------------- #

def main():
    USERNAME = os.getenv("BSKY_USERNAME")
    PASSWORD = os.getenv("BSKY_PASSWORD")
    if not USERNAME or not PASSWORD:
        print("Missing BSKY_USERNAME / BSKY_PASSWORD")
        return

    cfg = load_json(CONFIG_PATH, {})
    max_reposts = int(cfg.get("max_total_per_run", 100))
    max_per_user = int(cfg.get("max_per_author_per_run", 3))
    delay = float(cfg.get("delay_seconds", 2))

    fetch_limit_feed = int(cfg.get("fetch_limit_per_feed", 80))
    fetch_limit_list_member_posts = int(cfg.get("fetch_limit_per_list", 20))  # per member
    search_limit_tag = int(cfg.get("search_limit_per_tag", 80))

    overlap_minutes = int(cfg.get("overlap_minutes", 15))
    fallback_hours = int(cfg.get("fallback_hours_first_run", 3))

    blocked = normalize_blocked_users(cfg.get("blocked_users", []))

    reposted = load_reposted()
    state = load_json(STATE_FILE, {"last_run_iso": ""})
    last_run_ts = parse_time(state.get("last_run_iso"))

    try:
        c = Client()
        c.login(USERNAME, PASSWORD)
    except Exception:
        print("Login fout")
        return

    # normalize sources URL-proof
    feeds_raw = cfg.get("feeds", [])
    lists_raw = cfg.get("lists", [])
    tags = [t.strip() for t in (cfg.get("hashtags", []) or []) if isinstance(t, str) and t.strip()]
    single_raw = (cfg.get("single_post_uri") or "").strip()

    feeds = [normalize_feed_uri(c, x) for x in feeds_raw if isinstance(x, str) and x.strip()]
    feeds = [f for f in feeds if f]
    lists = [normalize_list_uri(c, x) for x in lists_raw if isinstance(x, str) and x.strip()]
    lists = [l for l in lists if l]
    single_post_uri = normalize_post_uri(c, single_raw) if single_raw else ""

    end_dt = now_utc()
    if last_run_ts:
        start_dt = last_run_ts - timedelta(minutes=overlap_minutes)
    else:
        start_dt = end_dt - timedelta(hours=fallback_hours)

    print(f"[INFO] Window: {start_dt.isoformat()} -> {end_dt.isoformat()}")
    print(f"[INFO] Feeds={len(feeds)} Lists={len(lists)} Tags={len(tags)} Blocked={len(blocked)}")

    # Collect candidates as tuples: (uri, cid, author_did, author_handle, created_dt, is_from_feed_or_list)
    candidates: Dict[str, Tuple[str, str, str, str, datetime, bool]] = {}

    per_user: Dict[str, int] = defaultdict(int)

    # ---- FEEDS (require #milf + media) ----
    for feed_uri in feeds:
        try:
            r = c.app.bsky.feed.get_feed(
                models.AppBskyFeedGetFeed.Params(feed=feed_uri, limit=fetch_limit_feed)
            )
        except Exception:
            continue

        for item in r.feed:
            if feed_item_is_repost(item):
                continue
            if not valid_common(item):
                continue

            dt = get_item_time(item)
            if dt < start_dt or dt > end_dt:
                continue

            text = getattr(item.post.record, "text", "") or ""
            if not has_milf_tag_in_text(text):
                continue

            uri = item.post.uri
            cid = item.post.cid
            author_did = item.post.author.did
            author_handle = item.post.author.handle

            if is_blocked(author_did, author_handle, blocked):
                continue
            if uri in reposted:
                continue

            candidates[uri] = (uri, cid, author_did, author_handle, dt, True)

    # ---- LISTS (Femdom-style: scan members, get posts_with_media, require #milf) ----
    for list_uri in lists:
        try:
            members = get_list_members(c, list_uri)
        except Exception:
            continue

        for did in members:
            if per_user.get(did, 0) >= max_per_user:
                continue

            try:
                r = c.app.bsky.feed.get_author_feed(
                    models.AppBskyFeedGetAuthorFeed.Params(
                        actor=did,
                        limit=fetch_limit_list_member_posts,
                        filter="posts_with_media",
                    )
                )
            except Exception:
                continue

            for item in r.feed:
                if feed_item_is_repost(item):
                    continue
                if not valid_common(item):
                    continue

                dt = get_item_time(item)
                if dt < start_dt or dt > end_dt:
                    continue

                text = getattr(item.post.record, "text", "") or ""
                if not has_milf_tag_in_text(text):
                    continue

                uri = item.post.uri
                cid = item.post.cid
                author_did = item.post.author.did
                author_handle = item.post.author.handle

                if is_blocked(author_did, author_handle, blocked):
                    continue
                if uri in reposted:
                    continue

                candidates[uri] = (uri, cid, author_did, author_handle, dt, True)

    # ---- HASHTAGS (search_posts, media-only; no #milf requirement) ----
    for tag in tags:
        q = tag if tag.startswith("#") else f"#{tag}"
        try:
            r = c.app.bsky.feed.search_posts(
                models.AppBskyFeedSearchPosts.Params(q=q, limit=search_limit_tag)
            )
        except Exception:
            continue

        for post in r.posts:
            rec = getattr(post, "record", None)
            if not rec:
                continue
            if is_reply_record(rec):
                continue
            if not media_ok(rec):
                continue

            dt = parse_time(getattr(post, "indexed_at", None)) or end_dt
            if dt < start_dt or dt > end_dt:
                continue

            uri = post.uri
            cid = post.cid
            author_did = post.author.did
            author_handle = post.author.handle

            if is_blocked(author_did, author_handle, blocked):
                continue
            if uri in reposted:
                continue

            candidates[uri] = (uri, cid, author_did, author_handle, dt, False)

    # Sort newest -> oldest
    sorted_items = sorted(candidates.values(), key=lambda x: x[4], reverse=True)

    # Build queue with per-user cap, reserve 1 slot for single post
    queue: List[Tuple[str, str, str, str]] = []
    reserve_for_single = 1 if single_post_uri else 0
    max_normal = max(0, max_reposts - reserve_for_single)

    for (uri, cid, author_did, author_handle, dt, _) in sorted_items:
        if len(queue) >= max_normal:
            break
        if per_user.get(author_did, 0) >= max_per_user:
            continue
        queue.append((uri, cid, author_did, author_handle))
        per_user[author_did] += 1

    # ---- SINGLE POST: unrepost -> repost, insert at position 3 ----
    single_tuple: Optional[Tuple[str, str, str, str]] = None
    if single_post_uri:
        sp = fetch_post_by_uri(c, single_post_uri)
        if sp:
            rec = getattr(sp, "record", None)
            if rec and (not is_reply_record(rec)) and media_ok(rec):
                author_did = sp.author.did
                author_handle = sp.author.handle
                if not is_blocked(author_did, author_handle, blocked):
                    # delete existing repost of this subject so we can repost again
                    deleted = delete_existing_repost_of_subject(c, USERNAME, single_post_uri)
                    if deleted:
                        print(f"[INFO] Single unrepost deleted={deleted}")
                    single_tuple = (sp.uri, sp.cid, author_did, author_handle)
            else:
                print("[WARN] Single post skipped: not media or is reply")
        else:
            print("[WARN] Single post not found")

    # Insert single at index 2 (positie 3)
    if single_tuple:
        idx = 2
        if idx > len(queue):
            idx = len(queue)
        queue = queue[:idx] + [single_tuple] + queue[idx:]
        queue = queue[:max_reposts]

    print(f"[INFO] Queue size: {len(queue)} (max={max_reposts})")

    # Execute reposts
    done = 0
    for i, (uri, cid, author_did, author_handle) in enumerate(queue, start=1):
        ok = do_repost_like(c, uri, cid)
        if ok:
            done += 1
            # single post should not be stored in reposted file (it cycles)
            if not (single_tuple and uri == single_tuple[0]):
                reposted.add(uri)
            print(f"[OK] {i:02d} reposted (by {author_handle})")
        else:
            print(f"[SKIP] {i:02d} failed repost {uri}")

        time.sleep(float(delay))

    # Save state
    save_reposted(reposted)
    save_json(STATE_FILE, {"last_run_iso": end_dt.isoformat().replace("+00:00", "Z")})

    print(f"✔ Run klaar | reposts={done} | tracked={len(reposted)}")


if __name__ == "__main__":
    main()