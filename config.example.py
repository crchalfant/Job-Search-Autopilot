"""
config.example.py  —  Job Search Autopilot configuration template

Copy this file to scripts/config.py and fill in your details.
scripts/config.py is gitignored and must never be committed.

Required environment variables (set in .env or system environment):
    ANTHROPIC_API_KEY   — from console.anthropic.com
    BRAVE_API_KEY       — from api.search.brave.com (free tier available)
    TAVILY_API_KEY      — from app.tavily.com (free tier available)
    ADZUNA_APP_ID       — from developer.adzuna.com (free)
    ADZUNA_APP_KEY      — from developer.adzuna.com (free)
    GMAIL_ADDRESS       — Gmail address used to send the digest
    GMAIL_APP_PW        — Gmail App Password (not your login password)
    USAJOBS_API_KEY     — from developer.usajobs.gov (free, optional)
    USAJOBS_EMAIL       — your email, required by USAJobs API (optional)
"""

from datetime import date

# ── SALARY FLOOR ──────────────────────────────────────────────────────────────
MIN_SALARY = 100000  # set this to your minimum acceptable annual salary (e.g. 120000 for $120K)

# ── VACATION MODE ─────────────────────────────────────────────────────────────
# Jobs are buffered during [VACATION_START, VACATION_END) and a single digest
# is sent on VACATION_END. Set both to past dates to disable vacation mode.
VACATION_START = date(2020, 1, 1)
VACATION_END   = date(2020, 1, 2)

# ── CANDIDATE PROFILE ─────────────────────────────────────────────────────────
# This is what Claude reads when rating every job. Be specific — the better
# your profile, the more accurate your ratings will be.
# Update whenever your role, targets, or priorities change.
PROFILE = """
Name: [Your Name]
Current role: [Your Title] at [Your Company], [Start Date] - present
[Bullet point: key achievement with a metric, e.g. "Grew X from Y to Z"]
[Bullet point: another achievement or area of expertise]

Target roles: [Your Target Role 1], [Your Target Role 2], [Your Target Role 3]

Target salary: $[YOUR_SALARY]+
Location: Remote (nationwide) OR on-site/hybrid in [Your City] area only

Hard constraints (always Skip):
- [Thing you will never do, e.g. "people management as primary responsibility"]
- [Industry or domain you want to avoid]
- [Any other hard no]

Core strengths:
- [Strength 1]
- [Strength 2]
- [Strength 3]
"""

# ── LOCAL METRO TERMS ────────────────────────────────────────────────────────
# City and suburb names for your commutable area. Jobs requiring on-site
# attendance outside this set are filtered out automatically.
# Replace with your own city and surrounding suburbs.
# Variable must be named LOCAL_METRO_TERMS — this name is used in job_autopilot.py.
LOCAL_METRO_TERMS = {
    "your city",
    "nearby suburb 1",
    "nearby suburb 2",
    "nearby suburb 3",
}

# ── CITY BLOCK PATTERNS ───────────────────────────────────────────────────────
# Raw regex strings used to detect onsite jobs outside your local metro.
# job_autopilot.py compiles these at startup.
#
# List cities you want to BLOCK (onsite roles you cannot commute to).
# Jobs with "remote" in the title, location, or description always pass
# through regardless of what these patterns match.
#
# URL_CITY_PATTERN scans job URLs for city slugs (e.g. /jobs/new-york/)
# LOC_CITY_PATTERN scans the location field for city names (e.g. "Chicago, IL")
#
# TIP: Use Claude or ChatGPT to generate a list for your country/region:
#   "Generate URL_CITY_PATTERN and LOC_CITY_PATTERN regex strings for a job
#    seeker based in [your city]. Block all major cities except [your metro]."

# Scans job URLs for city slugs (e.g. /jobs/san-francisco/)
URL_CITY_PATTERN = (
    # Add cities to block — use slug format (lowercase, hyphens or underscores)
    # r"new[-_]york|new%20york|manhattan"
    # r"|los[-_]angeles|los%20angeles"
    # r"|chicago|austin|boston|miami"
    # r"|london|paris|berlin|amsterdam"
    # r"|toronto|sydney|tokyo|singapore"
    r"(?!)"  # matches nothing — replace this with your city patterns
)

# Scans the job location field for city names (ATS jobs have no city in URL)
LOC_CITY_PATTERN = (
    # Add cities to block — use \b word boundaries
    # r"\b(new york|manhattan|brooklyn)\b"
    # r"|\b(los angeles|santa monica|irvine)\b"
    # r"|\bchicago\b|\baustin\b|\bboston\b"
    # r"|\blondon\b|\bparis\b|\bberlin\b"
    # r"|\btoronto\b|\bsydney\b|\btokyo\b"
    r"(?!)"  # matches nothing — replace this with your city patterns
)

# ── MOTIVATIONAL QUOTES ───────────────────────────────────────────────────────
# Shown in quiet-day emails when no new jobs come through.
# Add as many as you like — one is picked at random each quiet day.
QUOTES = [
    "The secret of getting ahead is getting started. - Mark Twain",
    "It always seems impossible until it's done. - Nelson Mandela",
    "You miss 100% of the shots you don't take. - Wayne Gretzky",
    # Add your own:
    # "Your favourite quote here.",
]

# ── TITLE BLOCK WORDS ────────────────────────────────────────────────────────
# Job titles that contain a TARGET_TITLES keyword but are actually the wrong
# role. If any of these words appear in a title, the job is dropped before
# it reaches Claude. Use lowercase — matching is case-insensitive.
# Common entries: wrong functions (engineer, designer, sales), wrong seniority
# (intern, associate), or staffing signals (contract, staffing).
TITLE_BLOCK_WORDS = [
    # Wrong function — replace or extend for your field
    # "engineer", "developer", "designer",
    # "marketing", "sales", "recruiter",
    # Wrong seniority / type
    # "intern", "associate",
    # Staffing signals
    # "staffing", "contract",
    "[your block word 1]",
    "[your block word 2]",
]

# ── TARGET JOB TITLES ────────────────────────────────────────────────────────
# Keywords used to filter ATS, Workday, Jobicy, RemoteOK, and UltiPro results.
# A job must contain at least one of these keywords in its title to be kept.
# Use lowercase partial keywords — "product manager" matches "Senior Product Manager".
# Update these to match your target role types.
TARGET_TITLES = [
    # Replace with your target role keywords. Examples:
    # "product manager", "product owner", "product lead",
    # "software engineer", "data scientist", "ux designer",
    # "marketing manager", "account executive", "financial analyst",
    "[your role keyword 1]",
    "[your role keyword 2]",
    "[your role keyword 3]",
]

# ── ATS COMPANY LIST ──────────────────────────────────────────────────────────
# Slugs for Greenhouse, Lever, and Ashby company job boards.
# The autopilot tries all three APIs for each slug automatically — you don't need
# to know which ATS a company uses, just add the slug.
#
# HOW TO FIND A SLUG:
#   Visit the company's careers page and look at the URL:
#   boards.greenhouse.io/SLUG  →  use SLUG
#   jobs.lever.co/SLUG         →  use SLUG
#   jobs.ashbyhq.com/SLUG      →  use SLUG
#
# If unsure, try the company name lowercased with no spaces. If that doesn't
# work, try with hyphens. Or ask Claude: "What is the ATS slug for [Company]?"
COMPANIES = [
    # Add slugs for companies you want to monitor directly.
    # The autopilot checks Greenhouse, Lever, and Ashby for each slug automatically.
    # Examples (replace with companies relevant to your search):
    # "stripe",
    # "notion",
    # "figma",
    # "company-slug",
]

# ── ATS NAME OVERRIDES ────────────────────────────────────────────────────────
# The autopilot generates display names from slugs automatically, but some slugs
# don't convert cleanly. Add corrections here: { "slug": "Display Name" }
ATS_NAME_OVERRIDES = {
    # "some-slug": "Correct Display Name",
}

# ── WORKDAY COMPANIES ─────────────────────────────────────────────────────────
# Companies that use Workday ATS. Format: (tenant, wd_server, site, display_name)
# Find these from the company's careers URL:
#   https://{tenant}.{wd_server}.myworkdayjobs.com/en-US/{site}
WORKDAY_COMPANIES = [
    # Examples (replace with companies relevant to your search):
    # ("jpmorgan",  "wd5",  "JPMorgan",  "JPMorgan Chase"),
    # ("microsoft", "wd5",  "Microsoft", "Microsoft"),
    # ("amazon",    "wd5",  "Amazon",    "Amazon"),
]

# ── DOMAIN -> COMPANY NAME MAP ────────────────────────────────────────────────
# Maps URL domain substrings -> company display names for Brave/Tavily results
# where the API returns no company field. First match wins.
# Add entries for companies in your target industry whose job URLs you expect
# to see in search results. Key = substring of the domain, value = display name.
# TIP: Add "{tenant}.wd" entries for each WORKDAY_COMPANIES tenant so that
# Brave/Tavily results linking to Workday pages get the correct company name.
DOMAIN_COMPANY_MAP = {
    # "yourcompany.com": "Your Company",
    # "tenant.wd": "Company Name",  # for each Workday company above
}

# ── JUNK URL PATTERNS ─────────────────────────────────────────────────────────
# URLs matching these patterns are generic hiring portals or landing pages,
# not actual job postings. Jobs with these URLs are auto-skipped before Claude.
JUNK_URL_PATTERNS = [
    # r"hiring\.amazon\.com/locations/",
]

# ── SEARCH QUERIES ────────────────────────────────────────────────────────────
# Replace [Your Role] and [Your Industry] with your actual target role keywords
# and industry. Use partial keywords — "product manager" matches "Senior Product
# Manager", "Lead Product Manager", etc.
#
# TIP: Upload this file to Claude or ChatGPT and ask it to generate tailored
# query lists based on your target roles and industry. See SETUP.txt for a
# ready-made prompt.

ADZUNA_QUERIES = [
    # Remote role queries — replace with your target role and industry
    "[Your Role] remote",
    "[Your Role] remote [Your Industry]",
    "senior [Your Role] remote",
    "lead [Your Role] remote",
    "[Your Role] remote [Your Industry] hiring",
    # Local area queries — replace your-city with your actual city
    "[Your Role] your-city",
    "senior [Your Role] your-city",
]

BRAVE_QUERIES = [
    # Written like natural search phrases for best results
    "senior [Your Role] remote [Your Industry] job opening",
    "[Your Role] remote [Your Industry] hiring",
    "lead [Your Role] remote [Your Industry]",
    "senior [Your Role] remote [Your Industry]",
    # Local queries
    "[Your Role] your-city job opening",
]

TAVILY_QUERIES = [
    "senior [Your Role] remote [Your Industry] job",
    "[Your Role] remote [Your Industry]",
    "lead [Your Role] remote [Your Industry] hiring",
    "senior [Your Role] remote [Your Industry] job opening",
    # Local queries
    "senior [Your Role] your-city",
]

LI_REMOTE_QUERIES = [
    # Short keyword phrases work best for LinkedIn
    "senior [Your Role]",
    "lead [Your Role]",
    "principal [Your Role]",
    "[Your Role] [Your Industry]",
    "senior [Your Role] [Your Industry]",
    "director [Your Role]",
]

LI_LOCAL_QUERIES = [
    # Local/hybrid searches — replace your-city and your-state
    "[Your Role] your-city",
    "senior [Your Role] your-state",
    "lead [Your Role] your-city",
    "[Your Role] your-city [Your Industry]",
]

HIMALAYAS_QUERIES = [
    # Format: ("category", "keyword")
    # NOTE: The category param is ignored server-side — only the keyword (q=)
    # is used for filtering. Keep the category for organizational purposes only.
    # Valid categories: product-management, business-analyst, management,
    #                   software-development, design, data-science, marketing
    ("management", "[your-role]"),
    ("management", "[your-role] [your-industry]"),
    # Add more keyword pairs for your role type:
    # ("your-category", "your keyword"),
]

USAJOBS_QUERIES = [
    # Keywords for USAJobs federal positions — only relevant if you want
    # to include government roles. Leave as-is or update for your field.
    "[Your Role]",
    "[Your Industry] specialist",
    "IT specialist",
]

JOBICY_QUERIES = [
    # Format: geo, industry, tag, count
    # Valid industries: management, accounting-finance, business, tech, design,
    #                   marketing, hr, legal, healthcare
    {"geo": "usa", "industry": "management", "tag": "[your role]",          "count": 50},
    {"geo": "usa", "industry": "management", "tag": "[your role] [industry]","count": 50},
    # Add more queries for your industry:
    # {"geo": "usa", "industry": "your-industry", "tag": "your keyword", "count": 50},
]

# ── HARD DISQUALIFIERS ────────────────────────────────────────────────────────
# Phrases that auto-skip a job before Claude is called.
# Scanned against title + description. Keep SHORT — only 100% unambiguous terms.
# Everything context-dependent is better handled by Claude.
HARD_DISQUALIFIERS = frozenset([
    # Staffing agency description phrases
    "on behalf of our client",
    "on behalf of a client",
    "our client is looking",
    "our client is seeking",
    # Add domain-specific terms you never want, e.g.:
    # "blockchain", "web3", "cryptocurrency", "nft", "smart contract",
    # "merchant cash advance",
])

# Regex pattern for word-boundary disqualifiers (avoids false positives).
# Use this for terms where a plain substring match would cause false positives.
# Example: r"\bdefi\b|\bcrypto\b" matches "defi" but not "define" or "definitely".
# Set to r"(?!)" (matches nothing) if you have no regex disqualifiers.
HARD_DISQ_PATTERN = r"(?!)"  # replace with e.g. r"\bdefi\b|\bcrypto\b"

# ── COMPANY PRE-FILTER ────────────────────────────────────────────────────────
# Staffing agencies and aggregators that are always-Skip regardless of title.
# Format: "company name substring": ("category", "reason string")
# Substring match is case-insensitive.
COMPANY_PREFILTER = {
    # ── Staffing agencies ─────────────────────────────────────────────────────
    "jobgether":      ("staffing", "Jobgether posts on behalf of partner companies — staffing agency hard disqualifier"),
    "insight global": ("staffing", "Insight Global is a staffing/recruiting firm — hard disqualifier"),
    "apex systems":   ("staffing", "Apex Systems is a staffing/IT recruiting firm — hard disqualifier"),
    "robert half":    ("staffing", "Robert Half is a staffing/recruiting firm — hard disqualifier"),
    # Add more as you encounter them:
    # "agency name": ("staffing", "Reason it is always Skip"),
    # ── Aggregators ───────────────────────────────────────────────────────────
    "ladders":        ("aggregator", "Ladders is a job aggregator that reposts listings — not a direct employer"),
}

# ── WRONG TITLE PATTERNS ──────────────────────────────────────────────────────
# Regex fragments for job titles that should always be filtered out before
# Claude sees them. Add patterns for wrong-function titles that keep appearing.
# Each entry is a raw string regex pattern — joined with | at startup.
WRONG_TITLE_PATTERNS = [
    # Wrong seniority — usually safe to filter for most searches
    r"\bjunior\b",
    r"\bintern(ship)?\b",
    r"\bentry.?level\b",
    # Add patterns for wrong-function titles you keep seeing:
    # r"\byour pattern here\b",
]
