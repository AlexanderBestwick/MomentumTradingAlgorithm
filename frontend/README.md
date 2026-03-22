# Frontend Starter

This folder is a lightweight starter dashboard for your trading project.

## What it is

- A static frontend you can open immediately without installing Node.js
- A clean starting point for:
  - latest backtest history
  - error monitoring
  - backtest graphs and summaries

## How to open it

Open [index.html](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/frontend/index.html) in your browser.

If you want a local server instead of opening the file directly, from the repo root you can run:

```powershell
python -m http.server 8000
```

Then open:

```text
http://localhost:8000/frontend/
```

## Files

- `index.html`: page structure
- `styles.css`: layout and visual design
- `app.js`: placeholder dashboard data and rendering

## How to customize it

Run [Backtesting.py](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/Backtesting.py) and it will update:

- [frontend/data/backtest-history.json](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/frontend/data/backtest-history.json)

The frontend reads that file automatically.

For display tweaks, edit:

- [frontend/app.js](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/frontend/app.js)
- [frontend/styles.css](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/frontend/styles.css)

## Best next upgrade

When Node is installed, the smooth next step is to migrate this into a `Next.js` app and keep the same content sections:

- summary cards
- recent backtests
- error feed
- selected backtest chart

That will map cleanly to an AWS setup like:

- frontend: Amplify Hosting
- backend API: FastAPI on ECS
- scheduled worker: EventBridge + ECS task
- logs and alerts: CloudWatch + SNS
