# Start VoiceForge AI in 4 commands

## Step 1 — Open terminal in this folder, then:

```bash
npm install
```

## Step 2 — Add YOUR Vapi key to .env

Open the `.env` file and replace `vapi_your_key_here` with your real key:
```
VAPI_API_KEY=vapi_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```
Get your key at: https://dashboard.vapi.ai → API Keys

## Step 3 — Start the server

```bash
node server.js
```

## Step 4 — Open your browser

Go to: http://localhost:3000

---

## That's it! ✅

- Sign up on YOUR platform
- Create an agent (fill in your business details)
- Your server calls Vapi with YOUR key silently
- You get a real phone number
- Call it — your agent answers in Arabic or English!

---

## What's connected

| Customer does | What actually happens |
|---|---|
| Signs up | Account saved in voiceforge.db |
| Creates agent | Your server → Vapi API (your key) → assistant created |
| Gets phone number | Your server → Vapi → Twilio → number assigned |
| Calls the number | Vapi handles the call using your AI configuration |
| Views calls/transcripts | Your server fetches from Vapi using your key |

The customer NEVER sees Vapi. They only see YOUR platform.
