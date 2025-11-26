import asyncio
import websockets
import json
import os

async def test_client():
    # Default port 25370 as per server config
    uri = "ws://localhost:25370"
    # Default password as per server config
    password = os.getenv("WS_AUTH_PASSWORD", "change_me_please")
    
    print(f"Connecting to {uri}...")
    try:
        async with websockets.connect(uri) as websocket:
            print("✅ Connected!")
            
            # 1. Wait for auth_required message
            response = await websocket.recv()
            print(f"< Server says: {response}")
            
            # 2. Send authentication
            auth_msg = {
                "type": "auth",
                "token": password
            }
            await websocket.send(json.dumps(auth_msg))
            print(f"> Sent auth with token: {password}")
            
            # 3. Listen for messages
            print("🎧 Listening for signals...")
            while True:
                response = await websocket.recv()
                data = json.loads(response)
                
                msg_type = data.get('type')
                print(f"\n< Received Type: {msg_type}")
                
                if msg_type == 'signals':
                    count = data.get('count', 0)
                    print(f"  🔥 SIGNALS RECEIVED: {count}")
                    for s in data.get('data', []):
                        print(f"    - {s['pair_symbol']} | Score: {s['total_score']} | Time: {s['timestamp']}")
                        print(f"      Patterns: {s.get('patterns')}")
                
                elif msg_type == 'auth_success':
                    print("  ✅ Authentication successful!")
                    
                elif msg_type == 'ping':
                    await websocket.send(json.dumps({'type': 'pong'}))
                    print("  > Sent pong")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure the server is running!")

if __name__ == "__main__":
    try:
        asyncio.run(test_client())
    except KeyboardInterrupt:
        print("\nStopped by user")
