"""
Pipeline utility functions shared across all karaoke-mvp services.

Added Redis Streams topics & helpers for event-driven pipelines:
  - STREAM_QUEUED
  - STREAM_METADATA_DONE
  - STREAM_SPLIT_DONE
  - STREAM_PACKAGED
  - STREAM_ORGANIZED

Publish with `publish_*`, consume with `consume`.
"""

import os
import logging
import redis
import requests
import smtplib
from email.message import EmailMessage
import traceback
import datetime
import time

# -------- LOGGING SETUP --------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}
logging.basicConfig(
    level=LEVELS.get(LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# -------- ENV VARS --------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
SLACK_WEBHOOK_URL   = os.environ.get("SLACK_WEBHOOK_URL")
NOTIFY_EMAILS       = os.environ.get("NOTIFY_EMAILS")
SMTP_SERVER         = os.environ.get("SMTP_SERVER")
SMTP_PORT           = int(os.environ.get("SMTP_PORT", 587))
SMTP_USERNAME       = os.environ.get("SMTP_USERNAME")
SMTP_PASSWORD       = os.environ.get("SMTP_PASSWORD")
REDIS_HOST          = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT          = int(os.environ.get("REDIS_PORT", 6379))

# -------- DIRECTORY CONFIG --------
QUEUE_DIR  = os.environ.get("QUEUE_DIR", "/queue")
META_DIR   = os.environ.get("META_DIR", "/metadata")
STEMS_DIR  = os.environ.get("STEMS_DIR", "/stems")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/output")
ORG_DIR    = os.environ.get("ORG_DIR", "/organized")
INPUT_DIR  = os.environ.get("INPUT_DIR", "/input")
LOGS_DIR   = os.environ.get("LOGS_DIR", "/logs")

# -------- REDIS CLIENT (singleton) --------
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# -------- REDIS STREAM KEYS --------
STREAM_QUEUED          = "stream:queued"
STREAM_METADATA_DONE   = "stream:metadata_done"
STREAM_SPLIT_DONE      = "stream:split_done"
STREAM_PACKAGED        = "stream:packaged"
STREAM_ORGANIZED       = "stream:organized"

# -------- STRING SANITIZATION --------
def clean_string(s):
    """Sanitize input for safe filesystem usage."""
    if not isinstance(s, str):
        s = str(s)
    return s.replace("\x00", "").replace("/", "-").replace("\\", "-").strip()

# -------- STATUS & ERROR MANAGEMENT (HASHES) --------
def set_file_status(filename, status, error=None, extra=None):
    key = f"file:{filename}"
    value = {"status": status}
    if error:
        value["error"] = error
    if extra:
        value.update(extra)
    try:
        redis_client.hset(key, mapping=value)
    except Exception as e:
        logger.error(f"Redis set_file_status error: {e}")

def get_files_by_status(status):
    try:
        keys = redis_client.keys("file:*")
    except Exception as e:
        logger.error(f"Redis get_files_by_status error: {e}")
        return []
    out = []
    for key in keys:
        try:
            data = redis_client.hgetall(key)
            if data.get("status") == status:
                out.append(key.replace("file:", ""))
        except Exception:
            continue
    return out

def set_file_error(filename, error):
    set_file_status(filename, "error", error=error)

def clear_file_error(filename):
    key = f"file:{filename}"
    try:
        redis_client.hset(key, "status", "queued")
        for stage in ["metadata", "splitter", "packager", "organizer"]:
            redis_client.delete(f"{stage}_retries:{filename}")
        redis_client.hdel(key, "error")
    except Exception as e:
        logger.error(f"Redis clear_file_error error: {e}")

# -------- NOTIFICATIONS --------
def send_telegram_message(message):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        try:
            resp = requests.post(url, data=data, timeout=5)
            if not resp.ok:
                logger.warning(f"Telegram notification failed: {resp.text}")
        except Exception as e:
            logger.warning(f"Telegram notification error: {e}")
    else:
        logger.debug("Telegram skipped")

def send_slack_message(message):
    if SLACK_WEBHOOK_URL:
        try:
            resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=5)
            if not resp.ok:
                logger.warning(f"Slack notification failed: {resp.text}")
        except Exception as e:
            logger.warning(f"Slack notification error: {e}")
    else:
        logger.debug("Slack skipped")

def send_email(subject, message):
    if NOTIFY_EMAILS and SMTP_SERVER and SMTP_USERNAME and SMTP_PASSWORD:
        try:
            msg = EmailMessage()
            msg.set_content(message)
            msg["Subject"] = subject
            msg["From"]    = SMTP_USERNAME
            msg["To"]      = [e.strip() for e in NOTIFY_EMAILS.split(",")]
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(msg)
        except Exception as e:
            logger.warning(f"Email notification error: {e}")
    else:
        logger.debug("Email skipped")

def notify_all(subject, message):
    send_telegram_message(message)
    send_slack_message(message)
    send_email(subject, message)

# -------- RETRY UTILITIES --------
def get_retry_count(stage, filename):
    try:
        return int(redis_client.get(f"{stage}_retries:{filename}") or 0)
    except:
        return 0

def increment_retry(stage, filename):
    cnt = get_retry_count(stage, filename) + 1
    try:
        redis_client.set(f"{stage}_retries:{filename}", cnt)
    except:
        pass
    return cnt

def reset_retry(stage, filename):
    try:
        redis_client.delete(f"{stage}_retries:{filename}")
    except:
        pass

def handle_auto_retry(stage, filename, func, max_retries=3, retry_delay=5, notify_fail=True):
    for attempt in range(1, max_retries + 1):
        try:
            result = func()
            reset_retry(stage, filename)
            return result
        except Exception as e:
            retries = increment_retry(stage, filename)
            tb = traceback.format_exc()
            timestamp = datetime.datetime.now().isoformat()
            set_file_error(filename, f"{timestamp}\n{e}\n{tb}")
            logger.error(f"{stage} error on {filename} (attempt {retries}): {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            elif notify_fail:
                notify_all(f"Pipeline Error [{stage}]", f"{stage} FAILED: {filename}\n{e}\n{tb}")
            if attempt == max_retries:
                raise

# -------- FILE STATUS SUMMARY --------
def get_file_status(filename):
    key = f"file:{filename}"
    try:
        data = redis_client.hgetall(key)
        return {
            "filename":   filename,
            "status":     data.get("status", "unknown"),
            "last_error": data.get("error", ""),
        }
    except Exception as e:
        return {"filename": filename, "status": "unknown", "last_error": str(e)}

# -------- REDIS STREAM HELPERS --------
def ensure_consumer_group(stream_key: str, group_name: str):
    """Create consumer group if it doesn’t already exist."""
    try:
        redis_client.xgroup_create(stream_key, group_name, id="$", mkstream=True)
    except redis.exceptions.ResponseError as e:
        # ignore “BUSYGROUP” if already created
        if "BUSYGROUP" not in str(e):
            logger.error(f"Error creating group {group_name} on {stream_key}: {e}")

def publish(stream_key: str, filename: str):
    """Add a message {'filename':filename} to the given stream."""
    try:
        redis_client.xadd(stream_key, {"filename": filename})
    except Exception as e:
        logger.error(f"Error publishing to {stream_key}: {e}")

def consume(stream_key: str, group_name: str, consumer_name: str,
            block: int = 0, count: int = 1):
    """
    Read next message(s) from a stream consumer-group.
    Returns list of (msg_id, data_dict).
    """
    try:
        entries = redis_client.xreadgroup(group_name, consumer_name,
                                          {stream_key: ">"}, block=block, count=count)
        # entries is [(stream_key, [(id, {b'filename': '...'}), ...])]
        if not entries:
            return []
        _, msgs = entries[0]
        return [(msg_id, {k.decode(): v for k, v in data.items()})
                for msg_id, data in msgs]
    except Exception as e:
        logger.error(f"Error consuming from {stream_key}: {e}")
        return []
