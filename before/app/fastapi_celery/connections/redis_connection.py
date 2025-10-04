# Standard Library Imports
import logging

# Third-Party Imports
import redis
from redis.exceptions import RedisError

import config_loader
from utils import log_helpers

# ===
# Set up logging
logger_name = "Redis Connection Config"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===

# === REDIS database === #
# Connect to Redis
redis_client = redis.Redis(
    host=config_loader.get_env_variable("REDIS_HOST", "localhost"),
    port=config_loader.get_env_variable("REDIS_PORT", 6379),
    password=config_loader.get_env_variable("REDIS_PASSWORD"),
    db=0,
    decode_responses=True,
)

# === Store per-task workflow step statuses in Redis === #
def store_step_status(task_id: str, step_name: str, status: str, step_id: str = None, ttl=3600):
    try:
        step_status_key = f"task:{task_id}:step_statuses"
        step_ids_key = f"task:{task_id}:step_ids"

        redis_client.hset(step_status_key, step_name, status)
        if step_id:
            redis_client.hset(step_ids_key, step_name, step_id)

        redis_client.expire(step_status_key, ttl)
        redis_client.expire(step_ids_key, ttl)

        return True
    except RedisError as e:
        logger.error(f"Redis error while storing step status for {task_id}: {e}")
        return False

def get_step_statuses(task_id: str):
    try:
        step_status_key = f"task:{task_id}:step_statuses"
        return redis_client.hgetall(step_status_key)
    except RedisError as e:
        logger.error(f"Redis error fetching step statuses for {task_id}: {e}")
        return {}

def get_step_ids(task_id: str):
    try:
        step_ids_key = f"task:{task_id}:step_ids"
        return redis_client.hgetall(step_ids_key)
    except RedisError as e:
        logger.error(f"Redis error fetching step IDs for {task_id}: {e}")
        return {}


# === Store and retrieve workflow ID associated with a task === #
def store_workflow_id(task_id: str, workflow_id: str, ttl=3600):
    try:
        key = f"task:{task_id}:workflow_id"
        redis_client.set(key, workflow_id, ex=ttl)
        return True
    except RedisError as e:
        logger.error(f"Redis error while storing workflow ID for {task_id}: {e}")
        return False

def get_workflow_id(task_id: str):
    try:
        key = f"task:{task_id}:workflow_id"
        return redis_client.get(key)
    except RedisError as e:
        logger.error(f"Redis error while fetching workflow ID for {task_id}: {e}")
        return None
