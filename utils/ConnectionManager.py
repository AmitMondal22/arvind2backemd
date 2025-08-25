from fastapi import WebSocket
from typing import Dict
class WsConnectionManager:
    """Class defining socket events"""
    
    def __init__(self):
        self.active_connections: Dict[int, WebSocket] = {}

    async def connect(self, user_id: int, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[user_id] = websocket

    def disconnect(self, user_id: int):
        del self.active_connections[user_id]

    async def send_personal_message(self, user_id: int, message: str):
        if user_id in self.active_connections:
            await self.active_connections[user_id].send_text(message)
    
    async def broadcast(self, message: str):
        """Broadcast message to all active connections"""
        for connection in self.active_connections:
            await connection.send_text(message)
            
            
            

    # def __init__(self):
    #     """init method, keeping track of connections"""
    #    self.active_connections: Dict[int, WebSocket] = {}

    # async def self_connect(self, websocket: WebSocket):
    #     """connect event"""
    #     await websocket.accept()
    #     self.active_connections.append(websocket)

    # async def self_send_personal_message(self, message: str, websocket: WebSocket):
    #     """Direct Message"""
    #     await websocket.send_text(message)

    # def self_disconnect(self, websocket: WebSocket):
    #     """disconnect event"""
    #     self.active_connections.remove(websocket)

    # async def self_broadcast(self, message: str):
    #     """Broadcast message to all active connections"""
    #     for connection in self.active_connections:
    #         await connection.send_text(message)
            