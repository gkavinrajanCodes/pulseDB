# client/cli.py

import requests
import sys
import json

BASE_URL = "http://localhost:8000"
API_KEY = "pulse-db-secret-key"

def send_command(command, args):
    headers = {"X-API-Key": API_KEY}
    try:
        resp = requests.post(
            f"{BASE_URL}/command",
            json={"command": command, "args": args},
            headers=headers
        )
        resp.raise_for_status()
        print(resp.json()["result"])
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pulsedb <command> [args...]")
        sys.exit(1)
    
    argv = list(sys.argv)
    command = argv[1].upper()
    args = argv[2:]
    
    if command == "SUBSCRIBE":
        print("Subscribing... (Use WebSocket for real-time)")
        # In a real CLI, we might use a websocket client here
        sys.exit(0)
        
    send_command(command, args)