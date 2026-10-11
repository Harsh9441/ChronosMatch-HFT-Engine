
# ChronosMatch-HFT-Engine
Zero-Copy high-frequency Trading ENgine in python
# ChronosMatch-HFT-Engine

ChronosMatch is a zero-copy high-frequency trading engine built in Python.  
It simulates buy/sell orders, matches them in real time, and displays stats on a live dashboard.

---

## 🚀 Setup Instructions
1. Install Python 3.10+ and Git.
2. Clone the repository:
   ```bash
   git clone https://github.com/your-repo.git
   cd ChronosMatch-HFT-Engine
# to checkout your branch
git checkout teammateX-dev

# Daily Commit Routine

git pull origin teammateX-dev
git add .
git commit -m "Day X: what you did"
git push origin teammateX-dev

# Team Roles (Week 1)
Teammate 1 → Ring buffer (mmap + struct)

Teammate 2 → Async firehose (asyncio)

Teammate 3 → Order book skeleton

Teammate 4 → Dashboard skeleton (curses)

# Day-wise Plan (Week 1)
# Day 1 (Setup & Basics)

T1: Ring buffer demo

T2: Firehose generator

T3: Order book skeleton

T4: Dashboard skeleton

# Day 2 (Integration)

T1: Connect buffer to firehose

T2: Push orders into buffer

T3: Insert logic for buy/sell

T4: Show “Orders received” counter

# Day 3 (Testing Flow)

T1: Test buffer speed

T2: Stress test firehose

T3: Retrieval from order book

T4: Show buffer usage %

# Day 4 (Refinement)

T1: Circular overwrite logic

T2: Realistic price/quantity ranges

T3: Draft matching logic

T4: Latency placeholder

# Day 5 (Milestone Check)

Run full pipeline: firehose → buffer → order book → dashboard

Each teammate commits daily progress with clear messages

# ✅ Contribution Rules
Work only in your assigned branch.

Push daily commits with clear messages.

Weekly merge into main after review.
