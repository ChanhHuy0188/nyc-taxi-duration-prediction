from fastapi import FastAPI

app = FastAPI()

@app.get("/hello")
async def hello():
    return {"message": "Hello, World!"}

# Add this for debugging
@app.get("/")
async def root():
    return {"message": "Root endpoint"}