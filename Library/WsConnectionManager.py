from fastapi import WebSocket
from typing import Dict, List

class WsConnectionManager:
    """Class defining socket events"""

    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, client_id: str, device_id: str, device: str, websocket: WebSocket):
        user_id = f"{client_id}-{device_id}-{device}"
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)

    def disconnect(self, websocket: WebSocket, client_id: str, device_id: str, device: str):
        user_id = f"{client_id}-{device_id}-{device}"
        if user_id in self.active_connections:
            self.active_connections[user_id] = [conn for conn in self.active_connections[user_id] if conn != websocket]
            if not self.active_connections[user_id]:  # If list is empty
                del self.active_connections[user_id]

    async def send_personal_message(self, client_id: str, device_id: str, device: str, message: str):
        user_id = f"{client_id}-{device_id}-{device}"
        if user_id in self.active_connections:
            for websocket in self.active_connections[user_id]:
                await websocket.send_text(message)

    async def broadcast(self, message: str):
        """Broadcast message to all active connections"""
        for connection in self.active_connections.values():
            for websocket in connection:
                await websocket.send_text(message)
