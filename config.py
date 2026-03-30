import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Стратегия
MIN_APR_THRESHOLD = float(os.getenv("MIN_APR_THRESHOLD", 100))         # минимальный APR для сигнала
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", 60))
MIN_OPEN_INTEREST_USD = float(os.getenv("MIN_OPEN_INTEREST_USD", 5_000_000))  # минимум $5M OI

# Hyperliquid
HYPERLIQUID_PRIVATE_KEY = os.getenv("HYPERLIQUID_PRIVATE_KEY")
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS")
POSITION_SIZE_USD = float(os.getenv("POSITION_SIZE_USD", 100))

# Backpack Exchange
BACKPACK_API_KEY = os.getenv("BACKPACK_API_KEY", "")     # Base64 Ed25519 public key
BACKPACK_API_SECRET = os.getenv("BACKPACK_API_SECRET", "") # Base64 Ed25519 private key

# BitMart Futures
BITMART_API_KEY = os.getenv("BITMART_API_KEY", "")
BITMART_API_SECRET = os.getenv("BITMART_API_SECRET", "")
BITMART_API_MEMO = os.getenv("BITMART_API_MEMO", "")

# Variational DEX
VARIATIONAL_TOKEN = os.getenv("VARIATIONAL_TOKEN", "")              # vr-token из браузера (начальный)
VARIATIONAL_WALLET = os.getenv("VARIATIONAL_WALLET", "")            # твой EVM адрес 0x...
VARIATIONAL_CF_CLEARANCE = os.getenv("VARIATIONAL_CF_CLEARANCE", "") # cf_clearance от Cloudflare
VARIATIONAL_PRIVATE_KEY = os.getenv("VARIATIONAL_PRIVATE_KEY", "")  # EVM приватный ключ для авто-обновления токена

# Extended Exchange (StarkNet)
EXTENDED_API_KEY = os.getenv("EXTENDED_API_KEY", "")
EXTENDED_PUBLIC_KEY = os.getenv("EXTENDED_PUBLIC_KEY", "")
EXTENDED_PRIVATE_KEY = os.getenv("EXTENDED_PRIVATE_KEY", "")
EXTENDED_VAULT_ID = int(os.getenv("EXTENDED_VAULT_ID", "0"))

# Автор
AUTHOR_CHANNEL = "https://t.me/hubcryptocis"
AUTHOR_CHANNEL_NAME = "@hubcryptocis"
DONATION_WALLET_EVM = "0xA3aCe3905fb080930f7Eeac9Fe401F5B41b16629"
DONATION_WALLET_SOL = "5UztCBoUq2HvtH5nibLmWgxuR5fU5AeagkX9mqdXa5Pq"
