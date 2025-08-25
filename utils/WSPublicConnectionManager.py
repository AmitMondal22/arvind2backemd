from fastapi import WebSocket
from typing import Dict

class WSPublicConnectionManager:
    """Class defining socket events"""

    def __init__(self):
        """init method, keeping track of connections"""
        self.active_connections: Dict[int, WebSocket] = {}
        self.counter = 0  # Counter for assigning unique IDs to connections

    async def self_connect(self, websocket: WebSocket):
        """connect event"""
        await websocket.accept()
        self.counter += 1
        self.active_connections[self.counter] = websocket

    async def self_send_personal_message(self, message: str, websocket: WebSocket):
        """Direct Message"""
        await websocket.send_text(message)

    def self_disconnect(self, websocket: WebSocket):
        """disconnect event"""
        for connection_id, connection in list(self.active_connections.items()):
            if connection == websocket:
                del self.active_connections[connection_id]
                break

    async def self_broadcast(self, message: str):
        """Broadcast message to all active connections"""
        for connection in self.active_connections.values():
            await connection.send_text(message)
