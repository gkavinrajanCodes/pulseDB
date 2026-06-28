"""
examples/leaderboard.py
========================
Demonstrates using PulseDB Sorted Sets for a real-time game leaderboard.

Pattern: Use ZADD to update scores, ZREVRANGE to get the top-N players,
and ZINCRBY to increment scores atomically.

This pattern replaces Redis ZADD for leaderboards, rate-limiting, and priority queues.

Requirements:
    pip install pulsedb
    # Start PulseDB server first:
    # docker run -d -p 6379:6379 -p 8000:8000 ghcr.io/gkavinrajancodes/pulsedb:latest
"""

import random
from pulsedb import PulseDB

db = PulseDB(host="localhost", port=6379)

LEADERBOARD_KEY = "game:leaderboard"


def record_score(player: str, score: float):
    """Record or update a player's score. If higher than current, replaces it."""
    current = db.execute_command("ZSCORE", LEADERBOARD_KEY, player)
    if current != "NULL" and float(current) >= score:
        print(f"  [{player}] Score {score} is not higher than existing {current} — skipped")
        return
    db.execute_command("ZADD", LEADERBOARD_KEY, str(score), player)
    print(f"  [{player}] Score updated to {score}")


def add_points(player: str, points: float) -> float:
    """Add points to a player's existing score atomically."""
    new_score = db.execute_command("ZINCRBY", LEADERBOARD_KEY, str(points), player)
    print(f"  [{player}] +{points} pts → total: {new_score}")
    return float(new_score)


def get_top(n: int = 10) -> list[tuple[str, float]]:
    """Get the top-N players in descending score order."""
    raw = db.execute_command("ZREVRANGE", LEADERBOARD_KEY, "0", str(n - 1), "WITHSCORES")
    return [(raw[i], float(raw[i+1])) for i in range(0, len(raw), 2)]


def get_rank(player: str) -> int | None:
    """Get a player's rank (1-indexed from top). Returns None if not on board."""
    total = db.execute_command("ZCARD", LEADERBOARD_KEY)
    rank = db.execute_command("ZRANK", LEADERBOARD_KEY, player)
    if rank == "NULL":
        return None
    return int(total) - int(rank)  # Convert ascending rank to descending


def get_players_in_score_range(min_score: float, max_score: float) -> list[str]:
    """Get all players with scores in a given range."""
    return db.execute_command("ZRANGEBYSCORE", LEADERBOARD_KEY, str(min_score), str(max_score))


if __name__ == "__main__":
    print("=== PulseDB Real-Time Leaderboard Demo ===\n")

    players = ["alice", "bob", "charlie", "dave", "eve"]

    # 1. Simulate initial scores
    print("--- Initial scores ---")
    for player in players:
        score = random.randint(100, 1000)
        record_score(player, score)

    # 2. Show top 5
    print("\n--- Top 5 Players ---")
    for rank, (player, score) in enumerate(get_top(5), 1):
        print(f"  #{rank} {player}: {score:.0f} pts")

    # 3. Alice earns bonus points
    print("\n--- Alice earns bonus points ---")
    add_points("alice", 500)
    add_points("alice", 250)

    # 4. Final leaderboard
    print("\n--- Final Leaderboard ---")
    for rank, (player, score) in enumerate(get_top(10), 1):
        arrow = " ← you" if player == "alice" else ""
        print(f"  #{rank} {player}: {score:.0f} pts{arrow}")

    # 5. Alice's rank
    alice_rank = get_rank("alice")
    print(f"\n  alice is currently ranked #{alice_rank}")

    # 6. Bronze tier players (score 200–500)
    print("\n--- Bronze Tier (200–500 pts) ---")
    bronze = get_players_in_score_range(200, 500)
    if bronze:
        print(f"  Players: {', '.join(bronze)}")
    else:
        print("  No players in this range")

    print("\n✅ Leaderboard demo complete!")
