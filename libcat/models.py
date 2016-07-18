import collections
import motor.motor_asyncio

class Db:
    def __init__(self, uri):
        self.uri = uri

    def __enter__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(self.uri)
        self.db = self.client.libcat
        return self

    def __exit__(self, type, value, traceback):
        self.client.close()
        pass

    async def find_package(self, name, path):
        query = {"name": name, "path": path}
        return await self.db.packages.find_one(query)

    async def save_package(self, name, path):
        model = {"name": name, "path": path}
        return await self.db.packages.save(model)

    async def find_binary(self, name):
        query = {"name": name}
        return await self.db.binaries.find_one(query)

    async def save_binary(self, name, symbols):
        model = {"name": name, "symbols": symbols}
        return await self.db.binaries.save(model)
