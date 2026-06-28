"""
examples/session_cache.py
=========================
Demonstrates using PulseDB as a high-performance session cache.

Pattern: Store authenticated user session tokens with TTL-based auto-expiry.
This pattern replaces Redis for session management in Flask/FastAPI apps.

Requirements:
    pip install pulsedb
    # Start PulseDB server first:
    # docker run -d -p 6379:6379 -p 8000:8000 ghcr.io/gkavinrajancodes/pulsedb:latest
"""

import json
import uuid
import time
from pulsedb import PulseDB

db = PulseDB(host="localhost", port=6379)


def create_session(user_id: str, user_data: dict, ttl_seconds: int = 3600) -> str:
    """Create a new authenticated session and return the session token."""
    token = str(uuid.uuid4())
    session_key = f"session:{token}"

    # Store session payload as JSON with an auto-expiring TTL
    db.set(session_key, json.dumps({
        "user_id": user_id,
        "created_at": time.time(),
        **user_data
    }), ttl=ttl_seconds)

    # Track active sessions for this user in a Set-like structure (using a Hash)
    db.hset(f"user:sessions:{user_id}", token, "1")

    print(f"[+] Session created for user '{user_id}' — token: {token[:8]}...")
    return token


def get_session(token: str) -> dict | None:
    """Retrieve session data for a token. Returns None if expired or invalid."""
    session_key = f"session:{token}"
    data = db.get(session_key)
    if not data or data == "NULL":
        return None
    return json.loads(data)


def invalidate_session(token: str) -> bool:
    """Log out: delete the session token immediately."""
    session_key = f"session:{token}"
    existing = db.get(session_key)
    if not existing or existing == "NULL":
        return False

    session = json.loads(existing)
    user_id = session.get("user_id")

    db.delete(session_key)
    if user_id:
        db.hdel(f"user:sessions:{user_id}", token)

    print(f"[-] Session {token[:8]}... invalidated.")
    return True


def get_session_ttl(token: str) -> int:
    """Check how many seconds remain on a session. -2 = expired."""
    return db.ttl(f"session:{token}")


if __name__ == "__main__":
    print("=== PulseDB Session Cache Demo ===\n")

    # 1. Create sessions
    token_a = create_session("alice", {"role": "admin", "email": "alice@example.com"}, ttl_seconds=30)
    token_b = create_session("bob",   {"role": "viewer", "email": "bob@example.com"},  ttl_seconds=60)

    # 2. Retrieve
    session = get_session(token_a)
    print(f"\n[GET] alice session: role={session['role']}, user_id={session['user_id']}")

    # 3. Check TTL
    print(f"[TTL] alice session expires in {get_session_ttl(token_a)}s")

    # 4. Invalid token
    bogus = get_session("totally-invalid-token")
    print(f"\n[GET] bogus token → {bogus}")

    # 5. Invalidate
    invalidate_session(token_a)
    print(f"[GET] after logout → {get_session(token_a)}")  # should be None

    print("\n✅ Session cache demo complete!")
