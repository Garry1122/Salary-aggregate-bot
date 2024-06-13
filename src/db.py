from motor.motor_asyncio import AsyncIOMotorClient
from src.config import MONGO_URI

client = AsyncIOMotorClient(MONGO_URI)
db = client.salary_db

async def test_connection():
    try:
        await db.command("ping")
        print("Connected to MongoDB")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
