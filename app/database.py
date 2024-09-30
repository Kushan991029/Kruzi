from model import User

import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://mongoadmin:kruzi@graphenelk.com/?retryWrites=true&w=majority")

database = client.kruzi
collection = database.user
collection1 = database.userLog

async def fetch_one_user(username):
    document = await collection.find_one({"username":username})
    return document

async def fetch_all_users():
    users = []
    cursor = collection.find({})
    async for document in cursor:
        users.append(User(**document))
    return users

async def create_user(user):
    document = user
    duplicateUser = await collection.find_one({"username":document['username']})
    if duplicateUser is None:
        result = await collection.insert_one(document)
        return "Success"
    else:
        return "username already exist"

async def update_user(username, password):
    await collection.update_one({"username":username},{"$set":{
        "password":password}})
    document = await collection.find_one({"username":username})
    return document

async def remove_user(username):
    await collection.delete_one({"username":username})
    return True
    
    
async def log_user(log):
    document = log
    result = await collection1.insert_one(log)
    return document