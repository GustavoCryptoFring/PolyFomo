# Polymarket Wallet Analyzer Bot

Telegram bot that analyzes Polymarket wallets and shows missed profit
(ATH analysis + resolution analysis).

## Setup on a server

```bash
git clone https://github.com/<YOUR_USERNAME>/<YOUR_REPO>.git
cd <YOUR_REPO>

# install dependencies
pip3 install -r requirements.txt --break-system-packages

# add your Telegram bot token (this file is git-ignored, never committed)
echo "PASTE_YOUR_BOT_TOKEN_HERE" > token.txt

# run
python3 wallet_bot.py
```

## Updating to a new version

```bash
cd <YOUR_REPO>
git pull
# then restart the bot
```

## Notes
- The bot token is read from `token.txt` (or the `TELEGRAM_BOT_TOKEN` env var).
  It is intentionally kept out of git.
- Telegram commands: `/start`, `/check 0x...`, `/pos`, `/help`.
