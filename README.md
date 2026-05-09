# Job Search Autopilot

A personal job search automation tool — and a practical introduction to vibe coding.

This project does two things. It runs every morning, searches 13+ job sources, rates every listing with Claude AI, and emails you a ranked digest. And it's designed so that anyone — developer or not — can pick it up, describe what they want to an AI, and make it their own without writing code from scratch.

If you're job hunting, it saves you hours of manual searching. If you're new to AI-assisted development, it's a real working project you can learn on.

## Screenshots
**Dashboard**
<img width="1258" height="1257" alt="Screenshot 2026-05-08 194320" src="https://github.com/user-attachments/assets/df77e34d-2c64-4fcc-b61f-6e77238b91c0" />


**Health tab**
<img width="1253" height="1263" alt="Screenshot 2026-05-08 194455" src="https://github.com/user-attachments/assets/9c0f39d3-19de-4be2-9296-5e13d50b0371" />
<img width="1259" height="1251" alt="Screenshot 2026-05-08 194530" src="https://github.com/user-attachments/assets/7789bed3-60d8-4ac2-8490-ece888dd8df2" />
<img width="1031" height="417" alt="Screenshot 2026-05-08 194543" src="https://github.com/user-attachments/assets/8675a972-bc0c-4aaa-bf04-032f8eab59c8" />


**Skipped jobs**
<img width="1256" height="1223" alt="Screenshot 2026-05-08 194359" src="https://github.com/user-attachments/assets/054be652-f3bd-4bfd-8a4d-0fcbc7a38a7d" />


**Example email digest**
<img width="1034" height="1157" alt="Screenshot 2026-05-08 185719" src="https://github.com/user-attachments/assets/464c3367-e266-4f6e-ad7d-f037616d3275" />


**Console output**
<img width="1659" height="1211" alt="Screenshot 2026-05-08 185814" src="https://github.com/user-attachments/assets/4485a746-1d76-4f76-a72e-6ba86c997e62" />



## What it does

- Searches 13+ sources: ATS direct (Greenhouse/Lever/Ashby), Workday, Adzuna, LinkedIn, Brave, Tavily, WeWorkRemotely, Jobicy, Himalayas, USAJobs, UltiPro/UKG, RemoteOK
- Filters out non-US roles, onsite roles outside your metro, below-salary-floor postings, staffing agencies, expired postings, and noise pages
- Extracts salary ranges from job descriptions when not provided as structured data
- Rates each job with Claude AI (Excellent Fit / Good Fit / Worth a Look / Skip) based on your profile
- Emails a daily digest sorted by tier
- Tracks your pipeline on a Kanban dashboard (Reviewing → Applied → Interviewing)
- Dashboard Health tab shows run history, source performance, and filter drilldown — click any filter pill to see which jobs were caught by it
- Skipped tab shows plain-English explanations of why each job was filtered

## Making it yours — vibe coding with AI

This tool is built to be customized. The filters, search queries, company list, and rating logic are all plain text or simple Python lists — readable and editable even if you are not a developer.

The fastest way to personalize it is to upload the files directly into Claude or ChatGPT and describe what you want in plain English. You do not need to understand every line of code. Just tell the AI what to change and it handles the syntax. This is vibe coding — you drive with plain English, the AI does the editing.

### How to do it

1. Go to [claude.ai](https://claude.ai) or [chatgpt.com](https://chatgpt.com)
2. Start a new conversation
3. Upload **both** of these files as attachments: `config.example.py` and `job_autopilot.py`
4. Paste the prompt below with your details filled in
5. Save the AI's response as `scripts/config.py`
6. Run it: `python scripts/job_autopilot.py --run`

**The one prompt that sets up everything:**
```
I've attached config.example.py and job_autopilot.py from a job search
automation tool. I need you to fill in config.example.py for my situation
so I can use it as my personal config.py file.

Here's my info:
  Name: [Your Name]
  Current role: [Your Title] at [Your Company]
  Target roles: [e.g. Software Engineer, Data Scientist, Marketing Manager]
  Target industries: [e.g. healthcare tech, SaaS, fintech]
  Target salary: $[amount]+
  Location: [e.g. "Remote nationwide" OR "Remote or hybrid in Chicago area"]
  My city and nearby suburbs: [list them]
  Things I never want: [e.g. contract roles, crypto, people management]
  My core strengths: [list 3-5]

Please fill in ALL sections of config.example.py and return the complete
file so I can save it directly as my config.py.
```

You can do multiple rounds — start with the basics and iterate from there. The AI can also help you debug errors, add new features, or explain what any part of the code does.

**Debugging tip:** If something breaks, paste the full error message plus the last 20 lines of console output into Claude or ChatGPT. You do not need to understand the error yourself — just describe what you were trying to do when it happened.

### More prompts for fine-tuning

**Set up for your role and location:**
```
I've attached job_autopilot.py and config.example.py. I'm a [your role] looking
for [target roles] in the [your city] area. Can you:
- Update TARGET_TITLES in config.py for my role type
- Set LOCAL_METRO_TERMS to my metro (city and nearby suburbs: [list them])
- Fill in all the search query lists with my actual target titles and industry
- Rewrite the PROFILE block for my background: [brief summary]
- Rewrite the Claude rating prompt tier definitions for my situation
```

**Add hard filters:**
```
I want to automatically skip jobs that mention [thing you hate], [industry
you don't want], and any contract positions. Can you add these to
HARD_DISQUALIFIERS in config.py and explain what each one does?
```

**Build your company watchlist:**
```
Can you help me find the Greenhouse, Lever, Ashby, and Workday ATS slugs for
these companies and add them to the COMPANIES and WORKDAY_COMPANIES lists in
config.example.py: [list]
```

**Tune the rating prompt:**
```
The Claude rating prompt in job_autopilot.py is generic. I'm a [your role].
Can you rewrite the Excellent Fit, Good Fit, Worth a Look, and Skip tier
definitions for my situation? My target roles are [roles]. My core
strengths are [strengths]. Things I won't do: [list].
```

**Debug a crash:**
```
I ran python scripts/job_autopilot.py --run and got this error:
[paste the full error message]

Here is the last 20 lines of console output:
[paste console output]

What is wrong and how do I fix it?
```

**Debug quiet days (no jobs in the email):**
```
The autopilot ran and sent a quiet day email — 0 jobs made it through. Here
is the filter breakdown from the console output:
[paste the filter breakdown lines]

My target roles are [your roles]. My location is remote nationwide OR
on-site in [your city]. What is likely causing everything to be filtered?
```

The `SETUP.txt` file has more detailed prompts for each specific part.

## What you can customize

### In `config.py` (your personal file, never committed to GitHub)

**`TARGET_TITLES`** — the job title keywords used to filter results from ATS boards, Workday, Jobicy, RemoteOK, and UltiPro. A job must contain at least one of these keywords in its title to be kept. Update this list to match your target role types.

**`TITLE_BLOCK_WORDS`** — words that disqualify a title even if it matched `TARGET_TITLES`. Use this to drop wrong-function roles (e.g. "engineer", "designer") or wrong-seniority roles (e.g. "intern") that sneak through.

**`PROFILE`** — the most important thing to personalize. Claude reads this when rating every single job. Write it like a concise professional summary: your current role, your background, what you are targeting, your strengths, and your hard constraints (location, salary, types of work you will not do). The more specific you are, the better the ratings.

**`MIN_SALARY`** — your salary floor as a plain number (e.g. `120000` for $120K). Jobs with a confirmed range entirely below this are filtered before Claude sees them. Jobs with no salary listed always pass through.

**`LOCAL_METRO_TERMS`** — your city and surrounding suburb names. The autopilot uses this to identify local hybrid and onsite roles and show them in a separate section of your email.

**`COMPANIES`** — the direct ATS company watchlist for Greenhouse, Lever, and Ashby. The autopilot checks all three for every company in this list on every run.

**`WORKDAY_COMPANIES`** — companies that use Workday ATS. Format: `(tenant, wd_server, site, display_name)`. Find a company's Workday URL on their careers page — it follows the pattern `https://{tenant}.{wdN}.myworkdayjobs.com/en-US/{site}`.

**`VACATION_START / VACATION_END`** — set both to past dates if you are not on vacation. During vacation the autopilot buffers results and sends one digest when you return.

**Search queries** (`ADZUNA_QUERIES`, `BRAVE_QUERIES`, `TAVILY_QUERIES`, `LI_REMOTE_QUERIES`, `LI_LOCAL_QUERIES`, `HIMALAYAS_QUERIES`, `JOBICY_QUERIES`) — tailor these to your exact job titles and industry keywords.

**`HARD_DISQUALIFIERS`** — phrases that auto-skip a job before it reaches Claude. Use this for things you are 100% certain you never want.

**`HARD_DISQ_PATTERN`** — a regex string for disqualifiers that need word-boundary matching (e.g. `r"\bdefi\b|\bcrypto\b"`).

**`COMPANY_PREFILTER`** — staffing agencies and job aggregators to block by company name.

**`WRONG_TITLE_PATTERNS`** — regex patterns that filter out wrong-function job titles before Claude sees them.

### In `job_autopilot.py`

**`SALARY_FLOOR_EXEMPT`** — companies you know pay well above your floor even when a search snippet shows a misleadingly low number.

**`_URL_CITY_RE` and `_LOC_CITY_RE`** — compiled from `URL_CITY_PATTERN` and `LOC_CITY_PATTERN` in `config.py`. Edit those config variables to control which cities are blocked for onsite roles.

**Claude rating prompt** — the tier definitions and rules Claude uses to score every job. Find the section starting with `"Perfect Fit" = 90-100%` in `job_autopilot.py`.

## Quick Start

> **New to this?** Read `SETUP.txt` for step-by-step instructions and troubleshooting tips. The steps below are the short version.

### 0. Download the project files

If you're on this page on GitHub and don't have the files yet:

1. Click the green **Code** button near the top of this page
2. Click **Download ZIP**
3. Unzip the downloaded file — you'll get a folder called `Job-Search-Autopilot-main` (or similar)
4. Rename it to `Job-Search-Autopilot` if you like, and move it somewhere easy to find (like your Desktop or Documents folder)

All the steps below are run from inside that folder.

> If you're comfortable with Git, you can also run `git clone https://github.com/crchalfant/Job-Search-Autopilot.git` instead of downloading the ZIP.

### 1. Install Python (if you haven't already)

You need Python 3.10 or later. Download it at [python.org/downloads](https://python.org/downloads). During installation on Windows, check the box that says **"Add Python to PATH"**.

### 2. Install dependencies

Open a terminal inside your project folder:
- **Windows:** Open the folder in File Explorer, click the address bar at the top, type `cmd`, and press Enter
- **Mac:** Right-click the folder in Finder → "New Terminal at Folder"

Then run:

```bash
pip install -r requirements.txt
```

If you get a "pip not found" error, try: `python -m pip install -r requirements.txt`

### 3. Set up API keys

**Windows:**
```
copy .env.example .env
```
**Mac/Linux:**
```bash
cp .env.example .env
```

Then open `.env` in a text editor (on Windows: right-click the file → Open With → Notepad) and fill in your API keys. See the **API keys needed** section below for where to get each one.

### 4. Set up your config

**Windows:**
```
copy config.example.py scripts\config.py
```
**Mac/Linux:**
```bash
cp config.example.py scripts/config.py
```

Then open `scripts/config.py` in a text editor and customize it for your job search. Use the AI prompts in the **Making it yours** section above to fill it in quickly — you can have Claude or ChatGPT do most of the work.

### 5. Run it

```bash
python scripts/job_autopilot.py --run
```

The first run takes 5–10 minutes. When it finishes, check your email inbox for the digest.

Add `--verbose` to see source latencies and filter breakdown. Full setup instructions are in `SETUP.txt`.

## Scheduling

**macOS/Linux** — add a cron job:

```cron
0 7 * * 1-5 cd /path/to/Job-Search-Autopilot && python scripts/job_autopilot.py --run
```

**Windows** — use Task Scheduler:
- Program: `python`
- Arguments: `scripts\job_autopilot.py --run`
- Start in: `C:\path\to\Job-Search-Autopilot`

## Running the dashboard

```bash
python scripts/dashboard.py
```

Then open [http://localhost:5000](http://localhost:5000) in your browser.

> **Important:** Keep the terminal window open the entire time you use the dashboard. The dashboard stops working if you close it. To stop the dashboard, press `Ctrl+C` in the terminal.

## API keys needed

Getting all the keys takes about 15 minutes.

### Anthropic (Claude AI) — required

Used to rate every job. The only paid service — costs roughly $1–5/month depending on run frequency.

1. Go to [console.anthropic.com](https://console.anthropic.com)
2. Create an account and add a payment method
3. Go to **API Keys** → **Create Key**
4. Copy the key — starts with `sk-ant-`

### Brave Search — required

1. Go to [api.search.brave.com](https://api.search.brave.com)
2. Create a free account → **API Keys** → **Create API Key** → select **Free** plan
3. Copy the key — starts with `BSA`

### Tavily — required

1. Go to [app.tavily.com](https://app.tavily.com)
2. Sign up for a free account — key shown on dashboard immediately
3. Copy the key — starts with `tvly-`

### Adzuna — required

1. Go to [developer.adzuna.com](https://developer.adzuna.com)
2. Register → **Dashboard** → **API Access Details**
3. Copy both your **App ID** and **App Key**

### Gmail App Password — required

1. Enable 2-Step Verification at [myaccount.google.com/security](https://myaccount.google.com/security)
2. Go to [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
3. Create an app password named `Job Search Autopilot`
4. Copy the 16-character password

### USAJobs — optional

1. Go to [developer.usajobs.gov](https://developer.usajobs.gov) → **Request an API Key**
2. Set both `USAJOBS_API_KEY` and `USAJOBS_EMAIL`

## Adding companies to the ATS list

### Greenhouse / Lever / Ashby

The `COMPANIES` list in `config.py` checks all three APIs automatically for each slug.

| ATS | URL pattern | Slug |
|---|---|---|
| Greenhouse | `boards.greenhouse.io/stripe` | `stripe` |
| Lever | `jobs.lever.co/plaid` | `plaid` |
| Ashby | `jobs.ashbyhq.com/mercury` | `mercury` |

### Workday

The `WORKDAY_COMPANIES` list in `config.py` covers companies that use Workday ATS. Format: `(tenant, wd_server, site, display_name)`. Find a company's Workday URL on their careers page — it follows the pattern `https://{tenant}.{wdN}.myworkdayjobs.com/en-US/{site}`.

## Project structure

```text
Job-Search-Autopilot/
├── scripts/
│   ├── job_autopilot.py     ← main autopilot (run this daily)
│   ├── dashboard.py         ← Flask Kanban dashboard
│   ├── autopilot_shared.py  ← shared utilities (IDs, normalization, validation)
│   └── config.py            ← your personal config (gitignored)
├── config.example.py        ← copy to scripts/config.py and fill in
├── .env.example             ← copy to .env and fill in API keys
├── requirements.txt
├── SETUP.txt                ← full setup guide with customization prompts
├── README.md
├── LICENSE
└── output/
    └── Job Search Autopilot/  ← runtime files (gitignored)
```

## Output files

All runtime files live in `output/Job Search Autopilot/` (gitignored).

| File | Purpose |
|---|---|
| `*-jobs.json` | Rated jobs for the dashboard board tab |
| `*-skipped.json` | Skipped jobs for the dashboard skipped tab |
| `*-report.md` | Email report attachment |
| `.seen.json` | Deduplication history (30-day rolling window) |
| `board_state.json` | Dashboard card positions and notes |
| `autopilot_runs.db` | SQLite run history for the Health tab |
| `debug_job_log.txt` | Full job log for debugging — upload to Claude to verify data accuracy |
| `vacation_buffer.json` | Jobs buffered during vacation — cleared automatically on return day |

## Vacation mode

Set `VACATION_START` and `VACATION_END` in `config.py` to your travel dates. The autopilot buffers results daily and sends one combined digest on your return day.

## License

MIT + Commons Clause. Free to use for personal job searching. Reach out before building anything commercial on top of it. See `LICENSE` for details.
