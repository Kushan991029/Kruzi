from model import User
from model import updateUser
from model import loginUser
from model import userLog
import uvicorn
from fastapi import FastAPI,HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError,jwt
# from jose import jwt
# import jwt
from datetime import datetime, timedelta
import os
import secrets

from influx_api import collect_data

app = FastAPI()

from database import (
    fetch_one_user,
    fetch_all_users,
    create_user,
    update_user,
    remove_user,
    log_user
)

SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

origins = ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials = True,
    allow_methods = ["*"],
    allow_headers = ["*"]
)

# | None = None

def create_access_token(data: dict, expires_delta: timedelta ):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)



@app.get("/")
def read_root():
    return {"Welcome":"Kruzi"}

@app.get("/api/user")
async def get_user():
    response = await fetch_all_users()
    return response

@app.get("/api/user/{username}", response_model=User)
async def get_user(username):
    response = await fetch_one_user(username)
    if response:
        return response
    raise HTTPException(404,f"There is no record with this username {username}")

@app.post("/api/user",response_model=User)
async def post_user(user:User):
    response = await create_user(user.dict())
    if response == "Success":
        return user
    elif response == "username already exist":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already exists",
        )
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Something went wrong / Bad request",
    )
    

@app.put("/api/user/{username}", response_model=updateUser)
async def put_user(username, user_update: updateUser):
    # Use the username from the path and the password from the request body
    response = await update_user(username,user_update.password)
    if response:
        return response
    raise HTTPException(404, f"There is no record with this username {username}")

@app.delete("/api/user/{username}")
async def delete_user(username):
    response = await remove_user(username)
    if response:
        return "Successfully deleted user"
    raise HTTPException(404,f"There is no record with this username {username}")

@app.post("/login")
async def login(user: loginUser):
    db_user = await fetch_one_user(user.username)
    if not db_user or db_user["password"] != user.password:
        raise HTTPException(status_code=400, detail="Invalid credentials")
    access_token = create_access_token(
        data={"sub": db_user["username"]}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/infapi/data/{site}/{start}/{stop}")
async def get_data(site:str,start:str,stop:str):
    response = await collect_data(site,start,stop)
    if response:
        return response
    raise HTTPException(404,f"There is no record with this username")


@app.post("/api/userLog",response_model=userLog)
async def log_of_users(user:userLog):
    user.timestamp = datetime.utcnow()
    response = await log_user(user.dict())
    if response:
        return user
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST
    )
    

if __name__ == '__main__':
    uvicorn.run(app, port=8088, host='0.0.0.0')