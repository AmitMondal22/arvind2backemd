import json
from fastapi import WebSocket, WebSocketDisconnect


class WsAlertConnectionManager:
    """WebSocket connection manager for alert notifications, keyed by user mobile number."""

    def __init__(self):
        self.active_connections = {}

    async def connect(self, mobile: str, websocket: WebSocket):
        """Connect a user by mobile number."""
        await websocket.accept()
        if mobile not in self.active_connections:
            self.active_connections[mobile] = []
        self.active_connections[mobile].append(websocket)
        print(f"Alert WS: Mobile {mobile} connected. Total: {len(self.active_connections[mobile])}")

    def disconnect(self, mobile: str, websocket: WebSocket):
        """Disconnect a user by mobile number."""
        if mobile in self.active_connections:
            self.active_connections[mobile] = [
                conn for conn in self.active_connections[mobile] if conn != websocket
            ]
            if not self.active_connections[mobile]:
                del self.active_connections[mobile]
            print(f"Alert WS: Mobile {mobile} disconnected.")

    async def send_alert_to_mobile(self, mobile: str, message: str):
        """Send alert message to a specific mobile user."""
        if mobile in self.active_connections:
            websockets_to_remove = []
            for websocket in self.active_connections[mobile]:
                try:
                    await websocket.send_text(message)
                except WebSocketDisconnect:
                    print(f"Alert WS: Disconnected for mobile {mobile}")
                    websockets_to_remove.append(websocket)
                except Exception as e:
                    print(f"Alert WS: Error sending to {mobile}: {e}")
                    websockets_to_remove.append(websocket)

            # Clean up disconnected websockets
            for ws in websockets_to_remove:
                if mobile in self.active_connections:
                    self.active_connections[mobile].remove(ws)
                    if not self.active_connections[mobile]:
                        del self.active_connections[mobile]
        else:
            print(f"Alert WS: Mobile {mobile} not connected.")

    async def broadcast_alert(self, message: str):
        """Broadcast alert to all connected users."""
        for mobile, connections in list(self.active_connections.items()):
            for websocket in connections:
                try:
                    await websocket.send_text(message)
                except Exception:
                    pass
