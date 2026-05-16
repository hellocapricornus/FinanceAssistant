# config.py
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("FINANCE_BOT_TOKEN")
OWNER_ID = int(os.getenv("FINANCE_OWNER", "0"))
