from fastapi import FastAPI
from contextlib import asynccontextmanager
from routers import api_file_processor, api_healthcheck
import config_loader

# Define the lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # # Startup logic
    # Application runs here, during this time the app is alive
    yield
    # Shutdown logic can go here if needed (after `yield`)

# Create the FastAPI app with the lifespan context manager
app = FastAPI(lifespan=lifespan, root_path="/fastapi")

# Include routers
app.include_router(api_healthcheck.router)
app.include_router(api_file_processor.router)

# Run the app with uvicorn (only when this script is executed directly)
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(config_loader.get_env_variable("APP_PORT", 8000)))
