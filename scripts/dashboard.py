"""
dashboard.py  -  Job Search Autopilot Dashboard
────────────────────────────────────────────────────────────────

A lightweight local Flask dashboard that reads the daily *-jobs.json and
*-skipped.json files written by job_autopilot.py and presents two views:

  Board tab   - Kanban board with three columns (Reviewing, Applied, Interviewing)
  Skipped tab - All auto-filtered jobs, with options to restore or flag for review

USAGE
  python scripts/dashboard.py   (or scripts\\dashboard.py on Windows)
  Then open http://localhost:5000 in your browser.
  Keep the terminal window open while using the dashboard.

REQUIRES
  pip install flask

FILES
  Reads:   output/job-autopilot/*-jobs.json          (written by job_autopilot.py)
           output/job-autopilot/*-skipped.json        (written by job_autopilot.py)
  Writes:  output/job-autopilot/board_state.json      (card positions, notes, rejections)
           output/job-autopilot/flagged_for_review.json (jobs flagged for Claude to review)

  All paths resolve from the project root (parent of scripts/) regardless of
  which directory this file is run from — see _HERE/_ROOT constants below.

BOARD COLUMNS
  Reviewing    - New jobs from the autopilot (default landing column)
  Applied      - Jobs you have submitted an application for
  Interviewing - Active interview in progress

SKIPPED TAB ACTIONS
  Add to Reviewing  - Moves the job onto the main board Reviewing column
  Flag for Review   - Saves to flagged_for_review.json with your note; upload
                      this file to your AI tool for filter re-review
  Delete (trash)    - Permanently hides the card from the skipped view

SECURITY NOTES (from code review 2026-03-18)
  - Job URLs use data-url attributes + openLinkSafe(), never injected into onclick
  - Job data for flagging uses _skipJobMap (JS Map), not inline JSON in onclick
  - _flagged_lock (threading.Lock) prevents race conditions on flagged file writes
  - All paths use _HERE/_ROOT so the app works from any working directory

PERSISTENCE
  board_state.json is written atomically (tmp + os.replace) after every action.
  All card positions, notes, applied dates, rejections, and flags survive restarts.
"""

import os
import re
import json
import glob
import sqlite3
import threading
from datetime import datetime
from flask import Flask, jsonify, request, render_template_string

from autopilot_shared import atomic_write_json, make_job_id, safe_json_load

# FIX (code review 2026-03-18): api_flag and api_unflag both do read-modify-write
# on flagged_for_review.json. Without a lock, two simultaneous requests (e.g. double-
# click) could both read the old file, both modify it, and the second write would
# silently discard the first change. _flagged_lock serialises all flagged file access.
_flagged_lock = threading.Lock()

# FIX (code review 2026-04-03): All board state routes do read-modify-write on
# board_state.json. Flask's dev server is single-threaded by default, but newer
# Flask versions may enable threading. _board_lock prevents concurrent writes
# corrupting board_state.json under threaded execution.
_board_lock = threading.Lock()
_cache_lock = threading.Lock()

app = Flask(__name__)

# ── Simple TTL cache for job/skipped file reads ────────────────────────────────
# FIX (code review): api_board() read all JSON files from disk on every page load
# with no caching. At 30+ files/month after 6 months this adds measurable latency.
# Cache the parsed job/skipped data for 30 seconds. Write actions invalidate it
# so board state changes appear immediately without waiting for TTL expiry.
import time as _time_module  # alias to avoid shadowing any local 'time' names

_jobs_cache: dict = {"jobs": None, "ids": None, "at": 0.0}
_skipped_cache: dict = {"jobs": None, "at": 0.0}
_CACHE_TTL = 30  # seconds — autopilot only writes once/day so 30s is safe

def _invalidate_job_cache():
    """Call after any write that changes what jobs appear on the board."""
    with _cache_lock:
        _jobs_cache["jobs"] = None
        _jobs_cache["ids"] = None
        _jobs_cache["at"] = 0.0
        _skipped_cache["jobs"] = None
        _skipped_cache["at"] = 0.0

def _cached_load_all_jobs():
    now = _time_module.time()
    with _cache_lock:
        if _jobs_cache["jobs"] is not None and now - _jobs_cache["at"] <= _CACHE_TTL:
            return list(_jobs_cache["jobs"]), dict(_jobs_cache["ids"])
    # Do disk I/O outside the lock so other threads are not blocked during file reads.
    jobs, ids = load_all_jobs()
    with _cache_lock:
        _jobs_cache["jobs"] = jobs
        _jobs_cache["ids"]  = ids
        _jobs_cache["at"]   = now
    return list(jobs), dict(ids)

def _cached_load_all_skipped():
    now = _time_module.time()
    with _cache_lock:
        if _skipped_cache["jobs"] is not None and now - _skipped_cache["at"] <= _CACHE_TTL:
            return list(_skipped_cache["jobs"])
    # Do disk I/O outside the lock.
    jobs = load_all_skipped()
    with _cache_lock:
        _skipped_cache["jobs"] = jobs
        _skipped_cache["at"]   = now
    return list(jobs)

# ── Paths ──────────────────────────────────────────────────────────────────────
# FIX (code review 2026-03-18): All paths previously used relative strings like
# "output/Job Search Autopilot/..." which resolved relative to wherever the script was run
# from. Running from scripts/ would look for scripts/output/... (wrong). Fixed
# by using __file__ to find the script's actual location, then stepping up one
# level to the project root. All 5 path constants now use os.path.join(_ROOT).
_HERE        = os.path.dirname(os.path.abspath(__file__))
_ROOT        = os.path.dirname(_HERE)   # parent of scripts/

JOBS_DIR     = os.path.join(_ROOT, "output", "Job Search Autopilot")
BOARD_STATE  = os.path.join(_ROOT, "output", "Job Search Autopilot", "board_state.json")
JOBS_GLOB    = os.path.join(_ROOT, "output", "Job Search Autopilot", "*-jobs.json")
SKIPPED_GLOB = os.path.join(_ROOT, "output", "Job Search Autopilot", "*-skipped.json")
FLAGGED_FILE = os.path.join(_ROOT, "output", "Job Search Autopilot", "flagged_for_review.json")
AUTOPILOT_DB = os.path.join(_ROOT, "output", "Job Search Autopilot", "autopilot_runs.db")

COLUMNS = ["Reviewing", "Applied", "Interviewing"]


# ── State helpers ──────────────────────────────────────────────────────────────

def load_board_state():
    """Load board state from disk. Returns dict keyed by job_id."""
    state = safe_json_load(BOARD_STATE, default={}, expected_type=dict)
    if not isinstance(state, dict):
        app.logger.warning("load_board_state: unexpected payload type, starting fresh")
        return {}
    return state


def save_board_state(state):
    """Atomically write board state to disk."""
    repaired = 0
    # Iterate over a snapshot of items() to avoid RuntimeError when repairing
    # malformed entries (modifying a dict during iteration raises in Python 3).
    for jid, s in list(state.items()):
        if not isinstance(s, dict):
            state[jid] = {}
            s = state[jid]
            repaired += 1
        if s.get("rejected") and s.get("column"):
            del s["column"]
            repaired += 1
    if repaired:
        app.logger.warning("save_board_state: repaired %d malformed entries", repaired)
    atomic_write_json(BOARD_STATE, state)


def load_all_jobs():
    """Read every *-jobs.json file, deduplicate by job_id, return list."""
    seen_ids = {}
    files = sorted(glob.glob(JOBS_GLOB), reverse=True)  # newest file wins on dedup
    for path in files:
        # Skip board_state.json if it ever matches the glob (it won't but safety first)
        if "board_state" in path:
            continue
        try:
            with open(path, encoding="utf-8") as f:
                jobs = json.load(f)
            if not isinstance(jobs, list):
                raise ValueError("jobs file must contain a list")
            for job in jobs:
                if not isinstance(job, dict):
                    continue
                jid = make_job_id(job)
                if jid not in seen_ids:
                    seen_ids[jid] = job
        except (ValueError, json.JSONDecodeError, OSError) as e:
            # FIX (code review): previously silent. Now logs so the user knows
            # why fewer jobs appear on the board than expected.
            app.logger.warning("load_all_jobs: skipping corrupt file %s: %s", path, e)
            continue

    return list(seen_ids.values()), seen_ids


def load_all_skipped():
    """Read every *-skipped.json file, deduplicate by job_id, return list newest-first.
    Uses 'entries' instead of 'jobs' for the per-file list to avoid shadowing the
    outer scope variable name (Fix 6 from code review).
    """
    seen_ids = {}
    files = sorted(glob.glob(SKIPPED_GLOB), reverse=True)
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                # FIX (code review 2026-03-18): Originally used 'jobs = json.load(f)'
                # which shadowed a common outer-scope variable name. Renamed to 'entries'.
                entries = json.load(f)
            if not isinstance(entries, list):
                raise ValueError("skipped file must contain a list")
            for job in entries:
                if not isinstance(job, dict):
                    continue
                jid = make_job_id(job)
                if jid not in seen_ids:
                    seen_ids[jid] = job
        except (ValueError, json.JSONDecodeError, OSError) as e:
            app.logger.warning("load_all_skipped: skipping corrupt file %s: %s", path, e)
            continue
    # Sort newest date_found first
    result = list(seen_ids.values())
    result.sort(key=lambda j: j.get("date_found", ""), reverse=True)
    return result


def build_board(jobs, state):
    """
    Merge jobs with their board state.
    Returns dict: { column_name: [job_with_state, ...] }
    Rejected jobs are excluded entirely.
    New jobs (not yet in state) land in 'Reviewing'.
    """
    columns = {col: [] for col in COLUMNS}

    for job in jobs:
        jid = make_job_id(job)
        card_state = state.get(jid, {})

        if card_state.get("rejected"):
            continue  # permanently hidden

        column = card_state.get("column", "Reviewing")
        if column not in COLUMNS:
            column = "Reviewing"

        enriched = dict(job)
        enriched["id"]          = jid
        enriched["column"]      = column
        enriched["applied_date"]= card_state.get("applied_date", "")
        enriched["flagged"]     = card_state.get("flagged", False)

        # Nice salary display — priority chain:
        # 1. Structured min/max (most reliable, from Adzuna/Jobicy/Himalayas/Workday)
        # 2. salary_extracted (parsed from description text)
        # 3. salary string field (raw string from source, e.g. Jobicy "salary" field)
        # 4. Empty string (no salary info)
        sal_min = job.get("salary_min") or 0
        sal_max = job.get("salary_max") or 0
        sal_ext = (job.get("salary_extracted") or "").strip()
        sal_str = (job.get("salary") or "").strip()
        if sal_min > 30000 and sal_max > 30000:
            enriched["salary_display"] = f"${sal_min:,.0f} – ${sal_max:,.0f}"
        elif sal_min > 30000:
            enriched["salary_display"] = f"${sal_min:,.0f}+"
        elif sal_ext:
            enriched["salary_display"] = sal_ext
        elif sal_str:
            enriched["salary_display"] = sal_str
        else:
            enriched["salary_display"] = ""

        columns[column].append(enriched)

    # Reviewing: tier priority first (Perfect Fit -> Good Fit -> Worth a Look),
    # then newest date_found first within each tier so today's jobs float to top.
    # Date strings are YYYY-MM-DD so we can negate lexical order with a minus sign
    # trick: sort by (tier_asc, date_desc) using a secondary reverse on date only.
    # Cleanest approach: sort with a key that uses tier ascending, date descending.
    tier_order = {"Perfect Fit": 0, "Good Fit": 1, "Worth a Look": 2}

    def _reviewing_key(j):
        tier = tier_order.get(j.get("tier", ""), 9)
        # Negate date lexically by inverting each character's ordinal value.
        # "2026-03-18" > "2026-03-17" so inverting makes newer dates sort first.
        # FIX (code review): validate the date is a well-formed YYYY-MM-DD ISO string
        # before inverting. Malformed dates (empty, "today", "2026/04/03") would
        # sort unpredictably. Fall back to "0000-00-00" on any format mismatch.
        date_str = j.get("date_found", "") or ""
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            date_str = "0000-00-00"
        date_neg = tuple(255 - ord(c) for c in date_str)
        return (tier, date_neg)

    columns["Reviewing"].sort(key=_reviewing_key)

    for col in ("Applied", "Interviewing"):
        columns[col].sort(
            key=lambda j: j.get("applied_date", "") or j.get("date_found", ""),
            reverse=True,
        )

    return columns


# ── Routes ─────────────────────────────────────────────────────────────────────

def _payload_dict():
    data = request.get_json(silent=True) or {}
    return data if isinstance(data, dict) else {}


def _validated_job_id(data):
    job_id = data.get("id")
    return job_id.strip() if isinstance(job_id, str) and job_id.strip() else None


def _validated_column(data):
    column = data.get("column")
    return column if column in COLUMNS else None


@app.route("/")
def index():
    return render_template_string(BOARD_HTML)


@app.route("/api/board")
def api_board():
    jobs, jobs_by_id = _cached_load_all_jobs()
    state            = load_board_state()

    # FIX: Skipped jobs that were restored via "Add to Reviewing" live in
    # *-skipped.json files, not *-jobs.json. load_all_jobs() never reads them,
    # so restored jobs never appeared on the board. Fix: find any board_state
    # entry marked as restored and not already in jobs_by_id, then pull its
    # data from the skipped files and inject it into the jobs list.
    restored_ids = {
        jid for jid, s in state.items()
        if s.get("restored") and s.get("column") and jid not in jobs_by_id
    }
    if restored_ids:
        skipped_by_id = {make_job_id(j): j for j in _cached_load_all_skipped()}
        for jid in restored_ids:
            if jid in skipped_by_id:
                jobs.append(skipped_by_id[jid])

    # FIX: Jobs moved to Applied/Interviewing can disappear after *-jobs.json
    # files are pruned (KEEP_RUNS=3 days). If a job_id is in board_state with
    # a pinned_job snapshot but is no longer in any *-jobs.json file, inject
    # the pinned snapshot back so the card stays visible on the board.
    for jid, s in state.items():
        if s.get("pinned_job") and s.get("column") and not s.get("rejected"):
            if jid not in jobs_by_id:
                pinned = dict(s["pinned_job"])
                jobs.append(pinned)
                jobs_by_id[jid] = pinned

    board  = build_board(jobs, state)
    total  = sum(len(v) for v in board.values())
    return jsonify({
        "columns": board,
        "total":   total,
        "last_updated": datetime.now().strftime("%b %d, %Y at %I:%M %p"),
    })


@app.route("/api/move", methods=["POST"])
def api_move():
    """Move a card to a different column.

    When a job is moved out of Reviewing (i.e. to Applied or Interviewing),
    we snapshot its core display fields into board_state.json. This ensures
    the card survives the daily *-jobs.json file pruning (KEEP_RUNS=3) and
    never disappears from the board just because its source file was deleted.
    """
    data   = _payload_dict()
    job_id = _validated_job_id(data)
    column = _validated_column(data)

    if not job_id or not column:
        return jsonify({"error": "invalid"}), 400

    with _board_lock:
        state = load_board_state()
        if job_id not in state:
            state[job_id] = {}
        state[job_id]["column"] = column
        if column == "Applied" and not state[job_id].get("applied_date"):
            state[job_id]["applied_date"] = datetime.now().strftime("%Y-%m-%d")

        # Persist job data so the card survives *-jobs.json pruning.
        # Only snapshot fields needed for display — skip large description.
        # job_data is sent by the frontend when dragging a card.
        job_data = data.get("job") if isinstance(data.get("job"), dict) else {}
        if job_data and column in ("Applied", "Interviewing"):
            # Q5: Always refresh the snapshot when moving to Applied/Interviewing.
            # Previously only written once — if a card was moved back to Reviewing
            # and then re-moved, the snapshot would be stale. Refresh every time
            # so the pinned data always reflects the most recent move.
            state[job_id]["pinned_job"] = {
                "title":            job_data.get("title", ""),
                "company":          job_data.get("company", ""),
                "location":         job_data.get("location", ""),
                "url":              job_data.get("url", ""),
                "source":           job_data.get("source", ""),
                "tier":             job_data.get("tier", ""),
                "reason":           job_data.get("reason", ""),
                "salary_min":       job_data.get("salary_min", 0),
                "salary_max":       job_data.get("salary_max", 0),
                "salary_extracted": job_data.get("salary_extracted", ""),
                "date_found":       job_data.get("date_found", ""),
            }

        try:
            save_board_state(state)
        except OSError as e:
            app.logger.error("api_move save_board_state failed: %s", e)
            return jsonify({"error": "disk write failed"}), 500
    return jsonify({"ok": True})


@app.route("/api/reject", methods=["POST"])
def api_reject():
    """Permanently hide a card from the board."""
    data   = _payload_dict()
    job_id = _validated_job_id(data)

    if not job_id:
        return jsonify({"error": "invalid"}), 400

    with _board_lock:
        state = load_board_state()
        if job_id not in state:
            state[job_id] = {}
        state[job_id]["rejected"] = True
        try:
            save_board_state(state)
        except OSError as e:
            app.logger.error("api_reject save_board_state failed: %s", e)
            return jsonify({"error": "disk write failed"}), 500
    return jsonify({"ok": True})


@app.route("/api/unreject", methods=["POST"])
def api_unreject():
    """Undo a reject — restore a card to the board.
    Clears the rejected flag so the card reappears in its previous column.
    Called by the persistent ↩ Undo delete button in the header.
    """
    data   = _payload_dict()
    job_id = _validated_job_id(data)
    if not job_id:
        return jsonify({"error": "invalid"}), 400
    with _board_lock:
        state = load_board_state()
        if job_id in state:
            state[job_id]["rejected"] = False
        try:
            save_board_state(state)
        except OSError as e:
            app.logger.error("api_unreject save_board_state failed: %s", e)
            return jsonify({"error": "disk write failed"}), 500
    return jsonify({"ok": True})




@app.route("/api/skipped")
def api_skipped():
    """Return all skipped jobs, excluding ones the user has already dismissed or restored."""
    skipped  = _cached_load_all_skipped()
    state    = load_board_state()
    # Exclude jobs the user has dismissed from skipped view, or already restored to board
    filtered = []
    for job in skipped:
        jid        = make_job_id(job)
        card_state = state.get(jid, {})
        if card_state.get("skip_dismissed") or card_state.get("column"):
            continue  # hidden or already on board
        filtered.append({**job, "id": jid, "flagged": card_state.get("flagged", False)})
    return jsonify({
        "jobs": filtered,
        "total": len(filtered),
        "last_updated": datetime.now().strftime("%b %d, %Y at %I:%M %p"),
    })


@app.route("/api/skip_dismiss", methods=["POST"])
def api_skip_dismiss():
    """Permanently hide a skipped job from the skipped view."""
    data   = _payload_dict()
    job_id = _validated_job_id(data)
    if not job_id:
        app.logger.warning("api_skip_dismiss: missing id in payload: %s", data)
        return jsonify({"error": "invalid", "detail": "missing id"}), 400
    with _board_lock:
        state = load_board_state()
        if job_id not in state:
            state[job_id] = {}
        state[job_id]["skip_dismissed"] = True
        try:
            save_board_state(state)
        except OSError as e:
            app.logger.error("api_skip_dismiss save_board_state failed: %s", e)
            return jsonify({"error": "disk write failed"}), 500
    app.logger.info("api_skip_dismiss: dismissed job_id=%r", job_id)
    return jsonify({"ok": True})


@app.route("/api/restore", methods=["POST"])
def api_restore():
    """Move a skipped job onto the Reviewing column of the main board."""
    data   = _payload_dict()
    job_id = _validated_job_id(data)
    if not job_id:
        return jsonify({"error": "invalid"}), 400
    with _board_lock:
        state = load_board_state()
        if job_id not in state:
            state[job_id] = {}
        state[job_id]["column"]   = "Reviewing"
        state[job_id]["restored"] = True
        # FIX (code review 2026-03-18): Previously also set skip_dismissed=False here,
        # but api_skipped filters restored jobs by checking card_state.get("column"),
        # not by skip_dismissed. Setting it False was dead code that added confusion.
        try:
            save_board_state(state)
        except OSError as e:
            app.logger.error("api_restore save_board_state failed: %s", e)
            return jsonify({"error": "disk write failed"}), 500
    _invalidate_job_cache()  # restored job must appear on next board load
    return jsonify({"ok": True})


@app.route("/api/flag", methods=["POST"])
def api_flag():
    """Flag a skipped job for Claude to re-review. Writes to flagged_for_review.json.
    _flagged_lock prevents concurrent writes corrupting the file on double-click."""
    data    = _payload_dict()
    job_id  = _validated_job_id(data)
    note    = data.get("note", "") if isinstance(data.get("note"), str) else ""
    job_data= data.get("job", {}) if isinstance(data.get("job"), dict) else {}
    if not job_id:
        return jsonify({"error": "invalid"}), 400

    with _flagged_lock:
        flagged = []
        if os.path.exists(FLAGGED_FILE):
            try:
                with open(FLAGGED_FILE, encoding="utf-8") as f:
                    flagged = json.load(f)
                    if not isinstance(flagged, list):
                        flagged = []
            except (json.JSONDecodeError, OSError):
                flagged = []

        existing_ids = {entry.get("id") for entry in flagged}
        if job_id not in existing_ids:
            flagged.append({
                "id":         job_id,
                "title":      job_data.get("title", ""),
                "company":    job_data.get("company", ""),
                "url":        job_data.get("url", ""),
                "reason":     job_data.get("reason", ""),
                "date_found": job_data.get("date_found", ""),
                "review_note": note,
                "flagged_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            })
            atomic_write_json(FLAGGED_FILE, flagged)

    with _board_lock:
        state = load_board_state()
        if job_id not in state:
            state[job_id] = {}
        state[job_id]["flagged"] = True
        try:
            save_board_state(state)
        except OSError as e:
            app.logger.error("api_flag save_board_state failed: %s", e)
            return jsonify({"error": "disk write failed"}), 500
    return jsonify({"ok": True})


@app.route("/api/unflag", methods=["POST"])
def api_unflag():
    """Remove a flag from a skipped job.
    _flagged_lock prevents concurrent writes corrupting the file."""
    data   = _payload_dict()
    job_id = _validated_job_id(data)
    if not job_id:
        return jsonify({"error": "invalid"}), 400

    with _flagged_lock:
        if os.path.exists(FLAGGED_FILE):
            try:
                with open(FLAGGED_FILE, encoding="utf-8") as f:
                    flagged = json.load(f)
                if not isinstance(flagged, list):
                    flagged = []
                flagged = [entry for entry in flagged if entry.get("id") != job_id]
                atomic_write_json(FLAGGED_FILE, flagged)
            except (json.JSONDecodeError, OSError):
                pass

    with _board_lock:
        state = load_board_state()
        if job_id in state:
            state[job_id]["flagged"] = False
        try:
            save_board_state(state)
        except OSError as e:
            app.logger.error("api_unflag save_board_state failed: %s", e)
            return jsonify({"error": "disk write failed"}), 500
    return jsonify({"ok": True})


@app.route("/api/health")
def api_health():
    """Return autopilot run history from SQLite for the Health tab.
    Returns the last 90 runs with per-source stats and filter breakdown.
    Returns empty runs list if DB does not exist yet (no runs completed).
    """
    if not os.path.exists(AUTOPILOT_DB):
        return jsonify({"runs": [], "sources": [], "last_updated": None})

    conn = None
    try:
        conn = sqlite3.connect(AUTOPILOT_DB, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")  # allows concurrent reads during autopilot writes

        runs_raw = conn.execute(
            "SELECT * FROM autopilot_runs ORDER BY id DESC LIMIT 30"
        ).fetchall()

        runs = []
        for run in runs_raw:
            run_id = run["id"]

            sources = [dict(r) for r in conn.execute(
                "SELECT source, raw_count, new_count, latency_ms "
                "FROM source_stats WHERE run_id=? ORDER BY raw_count DESC",
                (run_id,),
            ).fetchall()]

            filters = [dict(r) for r in conn.execute(
                "SELECT reason, count FROM filter_stats WHERE run_id=? ORDER BY count DESC",
                (run_id,),
            ).fetchall()]

            runs.append({
                "id":           run_id,
                "started_at":   run["started_at"],
                "finished_at":  run["finished_at"],
                "total_raw":    run["total_raw"],
                "total_new":    run["total_new"],
                "total_rated":  run["total_rated"],
                "report_file":  run["report_file"],
                "sources":      sources,
                "filters":      filters,
            })

        # Collect all source names seen across all runs for table headers
        all_sources = sorted({s["source"] for r in runs for s in r["sources"]})

        return jsonify({
            "runs":         runs,
            "sources":      all_sources,
            "last_updated": runs[0]["finished_at"] if runs else None,
        })

    except Exception as e:
        app.logger.error("api_health error: %s", e)
        return jsonify({"error": "health unavailable", "runs": [], "sources": [], "last_updated": None}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route("/api/health/filter/<reason>")
def api_health_filter(reason):
    """Return individual filtered jobs for a given filter reason, newest-run first."""
    if not os.path.exists(AUTOPILOT_DB):
        return jsonify({"jobs": [], "reason": reason})
    conn = None
    try:
        conn = sqlite3.connect(AUTOPILOT_DB, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        # Check table exists (older DBs won't have it yet)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if "filtered_job_details" not in tables:
            return jsonify({"jobs": [], "reason": reason,
                            "note": "No drilldown data yet — run the autopilot once and this will fill in."})
        rows = conn.execute(
            "SELECT d.title, d.company, d.url, d.source, r.started_at "
            "FROM filtered_job_details d "
            "JOIN autopilot_runs r ON r.id = d.run_id "
            "WHERE d.reason = ? "
            "ORDER BY r.id DESC, d.id ASC",
            (reason,),
        ).fetchall()
        jobs = [dict(r) for r in rows]
        return jsonify({"jobs": jobs, "reason": reason, "total": len(jobs)})
    except Exception as e:
        app.logger.error("api_health_filter error: %s", e)
        return jsonify({"error": "unavailable", "jobs": []}), 500
    finally:
        if conn is not None:
            conn.close()


@app.route("/api/health/query-stats")
def api_health_query_stats():
    """Return per-source query performance stats across all runs for the Health tab."""
    if not os.path.exists(AUTOPILOT_DB):
        return jsonify({"sources": []})
    conn = None
    try:
        conn = sqlite3.connect(AUTOPILOT_DB, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if "query_stats" not in tables:
            return jsonify({"sources": [],
                            "note": "No query data yet — run the autopilot once to populate."})
        rows = conn.execute(
            "SELECT source, SUM(raw_count) as raw, SUM(new_count) as new_jobs "
            "FROM query_stats GROUP BY source ORDER BY raw DESC"
        ).fetchall()
        # Sum up "seen" dedup counts per source from filtered_job_details so we can
        # exclude them from the filter rate (seen != filtered by a quality rule).
        # ATS jobs have source = "Greenhouse"/"Lever"/"Ashby" on individual jobs but
        # are stored as "ATS" in query_stats — normalise them to the query_stats key.
        _ATS_SUBSOURCES = {"Greenhouse", "Lever", "Ashby"}
        seen_by_source = {}
        if "filtered_job_details" in tables:
            seen_rows = conn.execute(
                "SELECT source, COUNT(*) as cnt "
                "FROM filtered_job_details "
                "WHERE reason = 'seen' "
                "GROUP BY source"
            ).fetchall()
            for sr in seen_rows:
                src = sr["source"]
                # Normalise ATS sub-sources to the "ATS" key used in query_stats
                key = "ATS" if src in _ATS_SUBSOURCES else src
                seen_by_source[key] = seen_by_source.get(key, 0) + sr["cnt"]
        sources = []
        for r in rows:
            raw = r["raw"] or 0
            new = r["new_jobs"] or 0
            seen_count = seen_by_source.get(r["source"], 0)
            # filtered = jobs actively rejected by a quality rule (not already-seen dedup)
            filtered = max(0, raw - new - seen_count)
            filter_rate = round(filtered / raw * 100) if raw > 0 else 0
            sources.append({
                "source":      r["source"],
                "raw":         raw,
                "new":         new,
                "filtered":    filtered,
                "filter_rate": filter_rate,
            })
        return jsonify({"sources": sources})
    except Exception as e:
        app.logger.error("api_health_query_stats error: %s", e)
        return jsonify({"error": "unavailable", "sources": []}), 500
    finally:
        if conn is not None:
            conn.close()
# Single-file design — no separate template directory needed.

BOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Job Search Autopilot</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:        #0f0f12;
    --surface:   #17171c;
    --surface2:  #1f1f27;
    --border:    rgba(255,255,255,.07);
    --text:      #e8e6f0;
    --muted:     #9594a8;
    --accent:    #7c6af5;
    --accent2:   #b49ef7;
    --perfect:   #f0c060;
    --good:      #6fcf8a;
    --look:      #63aadf;
    --applied:   #a78bfa;
    --interview: #f97316;
    --danger:    #e05a5a;
    --skipped:   #6b7280;
    --flagged:   #f59e0b;
    --col-w:     360px;
    --radius:    12px;
  }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'DM Sans', sans-serif;
    font-size: 14px;
    min-height: 100vh;
    overflow-x: auto;
  }

  /* ── Header ── */
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 16px 32px;
    border-bottom: 1px solid var(--border);
    background: var(--surface);
    position: sticky;
    top: 0;
    z-index: 100;
    gap: 20px;
  }

  .logo {
    font-family: 'DM Serif Display', serif;
    font-size: 22px;
    letter-spacing: -.3px;
    color: var(--text);
    flex-shrink: 0;
  }
  .logo span { color: var(--accent2); font-style: italic; }

  .header-stats {
    display: flex;
    gap: 20px;
    align-items: center;
    flex-wrap: wrap;
  }

  .stat-chip {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    color: var(--muted);
    white-space: nowrap;
  }
  .stat-chip strong {
    color: var(--text);
    font-weight: 600;
    font-size: 15px;
  }
  .stat-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
  }

  /* ── Tab nav ── */
  .tab-nav {
    display: flex;
    gap: 6px;
    align-items: center;
    flex-shrink: 0;
  }

  .tab-btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    font-family: 'DM Sans', sans-serif;
    font-weight: 500;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--muted);
    border-radius: 8px;
    padding: 6px 14px;
    cursor: pointer;
    transition: all .15s;
    white-space: nowrap;
  }
  .tab-btn:hover { border-color: rgba(255,255,255,.18); color: var(--text); }
  .tab-btn.active {
    background: rgba(124,106,245,.15);
    border-color: rgba(124,106,245,.4);
    color: var(--accent2);
  }
  .tab-btn.tab-skipped.active {
    background: rgba(107,114,128,.12);
    border-color: rgba(107,114,128,.35);
    color: var(--skipped);
  }

  .badge {
    background: var(--surface2);
    border-radius: 10px;
    padding: 1px 7px;
    font-size: 11px;
    font-weight: 600;
    color: var(--muted);
  }
  .tab-btn.active .badge { background: rgba(255,255,255,.08); color: var(--text); }

  /* Persistent undo button — appears in header after a delete, stays until used */
  .undo-btn {
    display: none;
    align-items: center;
    gap: 6px;
    background: rgba(224, 90, 90, 0.12);
    border: 1px solid rgba(224, 90, 90, 0.4);
    color: #e05a5a;
    border-radius: 6px;
    padding: 5px 12px;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    transition: background .15s;
  }
  .undo-btn:hover { background: rgba(224, 90, 90, 0.22); }
  .undo-btn.visible { display: flex; }

  /* Groups the undo button and timestamp at the right end of the header */
  .header-end {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .last-updated {
    font-size: 12px;
    color: var(--muted);
    white-space: nowrap;
  }

  /* ── Board layout ── */


  /* ── Dashboard hero / controls ── */
  .dashboard-shell {
    padding: 28px 28px 40px;
    max-width: 1680px;
    margin: 0 auto;
  }

  .hero {
    background:
      radial-gradient(circle at top left, rgba(124,106,245,.22), transparent 34%),
      radial-gradient(circle at top right, rgba(111,207,138,.12), transparent 28%),
      linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,0));
    border: 1px solid var(--border);
    border-radius: 24px;
    padding: 28px;
    margin-bottom: 22px;
    box-shadow: 0 22px 60px rgba(0,0,0,.32);
  }

  .hero-top {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 20px;
    margin-bottom: 24px;
  }

  .hero-copy { max-width: 760px; }

  .eyebrow {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 10px;
    margin-bottom: 14px;
    border-radius: 999px;
    border: 1px solid rgba(124,106,245,.28);
    background: rgba(124,106,245,.12);
    color: var(--accent2);
    font-size: 11px;
    letter-spacing: .09em;
    text-transform: uppercase;
    font-weight: 700;
  }

  .hero-title {
    font-size: 34px;
    line-height: 1.05;
    letter-spacing: -.04em;
    font-weight: 700;
    color: var(--text);
    margin-bottom: 8px;
  }

  .hero-subtitle {
    font-size: 15px;
    line-height: 1.6;
    color: var(--muted);
    max-width: 720px;
  }

  .hero-timestamp {
    min-width: 220px;
    text-align: right;
    color: var(--muted);
    font-size: 12px;
  }

  .hero-timestamp strong {
    display: block;
    color: var(--text);
    font-size: 14px;
    margin-top: 6px;
  }

  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 14px;
    margin-bottom: 18px;
  }

  .kpi-card {
    position: relative;
    overflow: hidden;
    border-radius: 18px;
    border: 1px solid var(--border);
    background: linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.01));
    padding: 16px 18px;
    min-height: 96px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.04);
  }

  .kpi-card::before {
    content: "";
    position: absolute;
    inset: 0 auto auto 0;
    width: 100%;
    height: 3px;
    background: var(--kpi-accent, var(--accent));
  }

  .kpi-label {
    color: var(--muted);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .08em;
    font-weight: 700;
  }

  .kpi-value {
    font-size: 30px;
    font-weight: 700;
    line-height: 1;
    color: var(--text);
  }

  .kpi-foot {
    color: var(--muted);
    font-size: 12px;
  }

  .board-toolbar {
    display: grid;
    grid-template-columns: minmax(260px, 1.5fr) repeat(3, minmax(150px, .7fr));
    gap: 12px;
  }

  .toolbar-field,
  .toolbar-select {
    width: 100%;
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,.08);
    background: rgba(9,11,18,.78);
    color: var(--text);
    padding: 13px 14px;
    font-size: 14px;
    outline: none;
    transition: border-color .15s, box-shadow .15s, background .15s;
  }

  .toolbar-field::placeholder { color: #7f7b94; }

  .toolbar-field:focus,
  .toolbar-select:focus {
    border-color: rgba(124,106,245,.55);
    box-shadow: 0 0 0 3px rgba(124,106,245,.15);
    background: rgba(12,14,24,.92);
  }

  .board {
    display: flex;
    gap: 20px;
    padding: 28px 32px 48px;
    align-items: flex-start;
  }

  .column {
    width: var(--col-w);
    flex-shrink: 0;
  }

  .col-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 14px;
    padding: 0 4px;
  }

  .col-title {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 1.2px;
    text-transform: uppercase;
    color: var(--muted);
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .col-count {
    background: var(--surface2);
    border-radius: 20px;
    padding: 2px 9px;
    font-size: 12px;
    font-weight: 600;
    color: var(--text);
  }

  .col-accent {
    display: block;
    width: 28px; height: 3px;
    border-radius: 2px;
  }
  .reviewing-accent   { background: var(--accent); }
  .applied-accent     { background: var(--good); }
  .interviewing-accent{ background: var(--interview); }

  .cards {
    display: flex;
    flex-direction: column;
    gap: 12px;
    min-height: 120px;
  }

  .empty-state {
    border: 1.5px dashed var(--border);
    border-radius: var(--radius);
    padding: 36px 20px;
    text-align: center;
    color: var(--muted);
    font-size: 13px;
  }

  /* ── Cards ── */
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px;
    transition: border-color .18s, box-shadow .18s;
  }

  .card:hover {
    border-color: rgba(124,106,245,.35);
    box-shadow: 0 4px 24px rgba(0,0,0,.35);
  }

  .card.flagged-card {
    border-color: rgba(245,158,11,.3);
  }
  .card.flagged-card:hover {
    border-color: rgba(245,158,11,.55);
  }

  .card-tier {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: .5px;
    margin-bottom: 8px;
    padding: 3px 8px;
    border-radius: 4px;
  }
  .tier-perfect { background: rgba(240,192,96,.12); color: var(--perfect); }
  .tier-good    { background: rgba(111,207,138,.12); color: var(--good); }
  .tier-look    { background: rgba(99,170,223,.12);  color: var(--look); }
  .tier-skip    { background: rgba(107,114,128,.1);  color: var(--skipped); }

  .card-title {
    font-size: 15px;
    font-weight: 600;
    line-height: 1.35;
    margin-bottom: 3px;
    color: var(--text);
  }

  .card-company {
    font-size: 13px;
    color: var(--muted);
    margin-bottom: 10px;
  }

  .card-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-bottom: 12px;
  }

  .meta-pill {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 11.5px;
    color: var(--muted);
    background: var(--surface2);
    border-radius: 4px;
    padding: 3px 7px;
  }

  .salary-pill  { color: var(--good);    background: rgba(111,207,138,.1); }
  .flagged-pill { color: var(--flagged); background: rgba(245,158,11,.1); }

  .card-reason {
    font-size: 12px;
    color: var(--muted);
    line-height: 1.5;
    margin-bottom: 12px;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    cursor: pointer;
    position: relative;
  }
  .card-reason::after {
    content: "…more";
    position: absolute;
    bottom: 0;
    right: 0;
    padding-left: 8px;
    background: var(--surface);
    color: var(--accent2);
    font-size: 11px;
  }
  .card-reason.reason-expanded::after { display: none; }
  .reason-expanded { -webkit-line-clamp: unset !important; overflow: visible !important; }


  /* ── Flag note textarea ── */
  .flag-note-area {
    width: 100%;
    background: var(--surface2);
    border: 1px solid rgba(245,158,11,.3);
    border-radius: 6px;
    color: var(--text);
    font-size: 12px;
    font-family: 'DM Sans', sans-serif;
    padding: 7px 10px;
    resize: vertical;
    min-height: 52px;
    margin-bottom: 8px;
    transition: border-color .15s;
  }
  .flag-note-area:focus { outline: none; border-color: var(--flagged); }
  .flag-note-area::placeholder { color: var(--muted); }

  /* ── Skipped list view ── */
  .skipped-view {
    padding: 28px 32px 48px;
    max-width: 900px;
  }

  .skipped-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 20px;
  }

  .skipped-title {
    font-family: 'DM Serif Display', serif;
    font-size: 22px;
    color: var(--text);
  }
  .skipped-subtitle {
    font-size: 13px;
    color: var(--muted);
    margin-top: 4px;
  }

  .skipped-list {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  .skip-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 16px;
    transition: border-color .15s;
  }
  .skip-card:hover { border-color: rgba(255,255,255,.12); }
  .skip-card.flagged-card { border-color: rgba(245,158,11,.3); }

  .skip-card-top {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 6px;
  }

  .skip-card-info { flex: 1; min-width: 0; }

  .skip-card-title {
    font-size: 14px;
    font-weight: 600;
    color: var(--text);
    margin-bottom: 2px;
  }

  .skip-card-company {
    font-size: 12px;
    color: var(--muted);
  }

  .skip-reason {
    font-size: 12px;
    color: var(--muted);
    line-height: 1.45;
    margin-bottom: 10px;
    background: var(--surface2);
    border-radius: 6px;
    padding: 7px 10px;
  }

  .skip-actions {
    display: flex;
    gap: 6px;
    align-items: center;
    flex-wrap: wrap;
  }

  /* ── Buttons ── */
  .btn {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 12px;
    font-family: 'DM Sans', sans-serif;
    font-weight: 500;
    border: 1px solid var(--border);
    background: var(--surface2);
    color: var(--muted);
    border-radius: 6px;
    padding: 5px 10px;
    cursor: pointer;
    transition: all .15s;
    white-space: nowrap;
  }
  .btn:hover { border-color: rgba(255,255,255,.18); color: var(--text); }

  .btn-primary {
    background: rgba(124,106,245,.15);
    border-color: rgba(124,106,245,.35);
    color: var(--accent2);
  }
  .btn-primary:hover { background: rgba(124,106,245,.25); }

  .btn-success {
    background: rgba(111,207,138,.1);
    border-color: rgba(111,207,138,.3);
    color: var(--good);
  }
  .btn-success:hover { background: rgba(111,207,138,.2); }

  .btn-orange {
    background: rgba(249,115,22,.1);
    border-color: rgba(249,115,22,.3);
    color: var(--interview);
  }
  .btn-orange:hover { background: rgba(249,115,22,.2); }

  .btn-flag {
    background: rgba(245,158,11,.1);
    border-color: rgba(245,158,11,.3);
    color: var(--flagged);
  }
  .btn-flag:hover { background: rgba(245,158,11,.2); }
  .btn-flag.flagged {
    background: rgba(245,158,11,.18);
    border-color: rgba(245,158,11,.5);
  }

  .btn-danger {
    background: transparent;
    border-color: transparent;
    color: var(--muted);
    margin-left: auto;
  }
  .btn-danger:hover { color: var(--danger); border-color: rgba(224,90,90,.3); }

  .btn-link {
    background: transparent;
    border-color: transparent;
    color: var(--accent2);
    padding-left: 2px;
    padding-right: 2px;
  }
  .btn-link:hover { text-decoration: underline; }

  .applied-badge {
    font-size: 11px;
    color: var(--muted);
    font-style: italic;
    margin-left: auto;
    align-self: center;
  }

  /* ── Flag panel (inline) ── */
  .flag-panel {
    display: none;
    margin-top: 10px;
    padding-top: 10px;
    border-top: 1px solid var(--border);
  }
  .flag-panel.open { display: block; }
  .flag-panel-label {
    font-size: 11px;
    font-weight: 600;
    color: var(--flagged);
    letter-spacing: .5px;
    text-transform: uppercase;
    margin-bottom: 6px;
  }
  .flag-panel-actions {
    display: flex;
    gap: 6px;
  }
  /* Board card flag panel — same visual language as skip-card flag-panel */
  .card-flag-panel {
    display: none;
    margin-top: 8px;
    padding-top: 8px;
    border-top: 1px solid var(--border);
  }
  .card-flag-panel.open { display: block; }

  /* ── Toast ── */
  .toast {
    position: fixed;
    bottom: 28px;
    right: 28px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 12px 18px;
    font-size: 13px;
    color: var(--text);
    box-shadow: 0 8px 32px rgba(0,0,0,.4);
    opacity: 0;
    transform: translateY(8px);
    transition: opacity .2s, transform .2s;
    pointer-events: none;
    z-index: 999;
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .toast.show { opacity: 1; transform: translateY(0); pointer-events: auto; }
  .toast-undo {
    background: transparent;
    border: 1px solid var(--muted);
    color: var(--text);
    border-radius: 4px;
    padding: 2px 10px;
    font-size: 12px;
    cursor: pointer;
    white-space: nowrap;
    flex-shrink: 0;
  }
  .toast-undo:hover { border-color: var(--text); }

  /* ── Loading ── */
  .loading {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 60vh;
    color: var(--muted);
    font-size: 14px;
    gap: 10px;
  }
  .spinner {
    width: 20px; height: 20px;
    border: 2px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin .7s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* ── Focus visible — keyboard accessibility ── */
  *:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }
  .toolbar-field:focus-visible,
  .toolbar-select:focus-visible {
    outline: none; /* these already have a custom focus ring via box-shadow */
  }

  /* ── Confirm overlay ── */
  .overlay {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,.65);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 200;
    opacity: 0;
    pointer-events: none;
    transition: opacity .2s;
  }
  .overlay.visible { opacity: 1; pointer-events: all; }

  .confirm-box {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 28px 32px;
    max-width: 360px;
    width: 90%;
    text-align: center;
  }
  .confirm-box h3 {
    font-family: 'DM Serif Display', serif;
    font-size: 20px;
    margin-bottom: 8px;
  }
  .confirm-box p {
    color: var(--muted);
    font-size: 13px;
    margin-bottom: 22px;
    line-height: 1.5;
  }
  .confirm-actions {
    display: flex;
    gap: 10px;
    justify-content: center;
  }
  .btn-confirm-reject {
    background: rgba(224,90,90,.15);
    border-color: rgba(224,90,90,.4);
    color: var(--danger);
    font-size: 13px;
    padding: 8px 20px;
  }
  .btn-confirm-reject:hover { background: rgba(224,90,90,.25); }

  svg { flex-shrink: 0; }

  /* ── Health tab ─────────────────────────────────────────────── */
  .health-view {
    padding: 24px;
    max-width: 1100px;
    margin: 0 auto;
  }
  .health-header {
    margin-bottom: 20px;
  }
  .health-title {
    font-size: 18px;
    font-weight: 600;
    color: var(--text);
  }
  .health-subtitle {
    font-size: 13px;
    color: var(--muted);
    margin-top: 2px;
  }
  .health-no-data {
    color: var(--muted);
    font-size: 14px;
    padding: 40px 0;
    text-align: center;
  }
  .health-table-wrap {
    overflow-x: hidden;
    overflow-y: auto;
    max-height: 360px;   /* ~10 rows visible; scroll to see more */
    margin-bottom: 28px;
    border-radius: 8px;
    border: 1px solid var(--border);
    position: relative;
  }
  .health-table-wrap[style*="overflow-x: auto"]::after {
    content: "→";
    position: absolute;
    top: 50%;
    right: 4px;
    transform: translateY(-50%);
    color: var(--muted);
    font-size: 16px;
    opacity: 0.5;
    pointer-events: none;
  }
  .health-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  .health-table thead th {
    position: sticky;
    top: 0;
    background: var(--surface2);
    z-index: 1;
  }
  .health-table th {
    text-align: left;
    padding: 8px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--muted);
    font-weight: 500;
    white-space: nowrap;
  }
  .health-table td {
    padding: 8px 12px;
    border-bottom: 1px solid rgba(255,255,255,.04);
    color: var(--text);
    white-space: nowrap;
  }
  .health-table tr:last-child td { border-bottom: none; }
  .health-table tr:hover td { background: rgba(255,255,255,.02); }
  .health-num { text-align: right; }
  .health-zero { color: var(--muted); }
  .health-section-title {
    font-size: 13px;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: .06em;
    margin: 24px 0 10px;
  }
  .health-filter-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-bottom: 20px;
  }
  .health-filter-pill {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 4px 12px;
    font-size: 12px;
    color: var(--muted);
    cursor: pointer;
    transition: border-color .15s, background .15s;
  }
  .health-filter-pill:hover { border-color: var(--accent); background: rgba(124,106,245,.08); }
  .health-filter-pill.active { border-color: var(--accent); background: rgba(124,106,245,.15); color: var(--text); }
  .health-filter-pill strong { color: var(--text); }

  .health-drilldown {
    margin-top: 16px;
    border: 1px solid var(--border);
    border-radius: 10px;
    overflow: hidden;
  }
  .health-drilldown-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 16px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    font-size: 13px;
    font-weight: 600;
  }
  .health-drilldown-close {
    background: none;
    border: none;
    color: var(--muted);
    cursor: pointer;
    font-size: 16px;
    line-height: 1;
    padding: 0 4px;
  }
  .health-drilldown-close:hover { color: var(--text); }
  .health-drilldown-list {
    max-height: 360px;
    overflow-y: auto;
  }
  .health-drilldown-item {
    display: flex;
    align-items: baseline;
    gap: 10px;
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
    font-size: 13px;
  }
  .health-drilldown-item:last-child { border-bottom: none; }
  .health-drilldown-item:hover { background: rgba(255,255,255,.02); }
  .health-drilldown-title { flex: 1; color: var(--text); }
  .health-drilldown-title a { color: var(--accent); text-decoration: none; }
  .health-drilldown-title a:hover { text-decoration: underline; }
  .health-drilldown-company { color: var(--muted); font-size: 12px; white-space: nowrap; }
  .health-drilldown-source { color: var(--muted); font-size: 11px; white-space: nowrap; }
  .health-drilldown-empty { padding: 20px 16px; color: var(--muted); font-size: 13px; text-align: center; }

  /* ── Query performance table ─────────────────────────────────────────── */
  .health-query-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    margin-top: 4px;
  }
  .health-query-table th {
    text-align: left;
    font-weight: 600;
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .05em;
    padding: 6px 12px;
    border-bottom: 1px solid var(--border);
  }
  .health-query-table th.hq-num { text-align: right; }
  .health-query-table td {
    padding: 7px 12px;
    border-bottom: 1px solid rgba(255,255,255,.04);
    color: var(--text);
  }
  .health-query-table td.hq-num { text-align: right; color: var(--muted); }
  .health-query-table tr:last-child td { border-bottom: none; }
  .health-query-table tr:hover td { background: rgba(255,255,255,.02); }
  .hq-bar-wrap {
    width: 80px;
    height: 6px;
    background: rgba(255,255,255,.08);
    border-radius: 3px;
    display: inline-block;
    vertical-align: middle;
    margin-left: 6px;
  }
  .hq-bar {
    height: 100%;
    border-radius: 3px;
    background: var(--accent);
    opacity: .7;
  }
  .hq-bar.high { background: #e05a5a; opacity: .8; }
  .hq-rate { font-size: 11px; color: var(--muted); }

  .health-hero {
    background:
      radial-gradient(circle at top left, rgba(124,106,245,.18), transparent 34%),
      radial-gradient(circle at top right, rgba(99,170,223,.14), transparent 28%),
      linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,0));
    border: 1px solid var(--border);
    border-radius: 24px;
    padding: 24px;
    margin-bottom: 18px;
    box-shadow: 0 18px 50px rgba(0,0,0,.26);
  }
  .health-summary-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 14px;
    margin-bottom: 18px;
  }
  .health-summary-card {
    border: 1px solid var(--border);
    border-radius: 18px;
    background: linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.01));
    padding: 16px 18px;
    min-height: 96px;
  }
  .health-summary-label {
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .08em;
    font-weight: 700;
    margin-bottom: 12px;
  }
  .health-summary-value {
    color: var(--text);
    font-size: 30px;
    font-weight: 700;
    line-height: 1;
    margin-bottom: 8px;
  }
  .health-summary-foot {
    color: var(--muted);
    font-size: 12px;
  }
  .health-panel {
    border: 1px solid var(--border);
    border-radius: 20px;
    background: linear-gradient(180deg, rgba(255,255,255,.025), rgba(255,255,255,.01));
    padding: 18px;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.03);
  }
  .health-table th {
    background: rgba(255,255,255,.02);
    position: sticky;
    top: 0;
    z-index: 1;
  }
  .skip-shell {
    background:
      radial-gradient(circle at top left, rgba(245,158,11,.09), transparent 28%),
      linear-gradient(180deg, rgba(255,255,255,.015), rgba(255,255,255,0));
    border: 1px solid var(--border);
    border-radius: 24px;
    padding: 24px;
    box-shadow: 0 18px 50px rgba(0,0,0,.26);
  }
  .skip-meta-row {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-top: 10px;
    margin-bottom: 12px;
  }
  .skip-reason {
    border: 1px solid rgba(255,255,255,.04);
    background: rgba(255,255,255,.03);
  }
  .skip-actions {
    justify-content: flex-start;
    padding-top: 2px;
  }

  @media (max-width: 1100px) {
    .health-summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .hero-top { flex-direction: column; }
    .hero-timestamp { text-align: left; min-width: 0; }
    .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .board-toolbar { grid-template-columns: 1fr 1fr; }
    .board { flex-direction: column; padding: 20px 16px; }
    .column { width: 100%; }
  }

  @media (max-width: 720px) {
    .dashboard-shell { padding: 18px 16px 28px; }
    .hero { padding: 20px; border-radius: 20px; }
    .hero-title { font-size: 28px; }
    .kpi-grid,
    .board-toolbar { grid-template-columns: 1fr; }
    header { flex-wrap: wrap; padding: 12px 16px; }
    .header-stats { display: none; }
    .tab-nav { width: 100%; justify-content: center; }
  }

</style>
</head>
<body>

<header role="banner">
  <div class="logo">Job Search <span>Autopilot</span></div>
  <div class="header-stats" id="header-stats" aria-label="Pipeline summary">
    <div class="stat-chip">
      <div class="stat-dot" style="background:var(--accent)" aria-hidden="true"></div>
      <span>Reviewing <strong id="count-reviewing">–</strong></span>
    </div>
    <div class="stat-chip">
      <div class="stat-dot" style="background:var(--good)" aria-hidden="true"></div>
      <span>Applied <strong id="count-applied">–</strong></span>
    </div>
    <div class="stat-chip">
      <div class="stat-dot" style="background:var(--interview)" aria-hidden="true"></div>
      <span>Interviewing <strong id="count-interviewing">–</strong></span>
    </div>
    <div class="stat-chip">
      <div class="stat-dot" style="background:var(--skipped)" aria-hidden="true"></div>
      <span>Skipped <strong id="count-skipped">–</strong></span>
    </div>
  </div>
  <nav class="tab-nav" role="tablist" aria-label="Dashboard views">
    <button class="tab-btn active" id="tab-board" role="tab" aria-selected="true" aria-controls="app" onclick="showTab('board')">
      Command Center <span class="badge" id="badge-board">0</span>
    </button>
    <button class="tab-btn tab-skipped" id="tab-skipped" role="tab" aria-selected="false" aria-controls="app" onclick="showTab('skipped')">
      ⛔ Skipped <span class="badge" id="badge-skipped">0</span>
    </button>
    <button class="tab-btn" id="tab-health" role="tab" aria-selected="false" aria-controls="app" onclick="showTab('health')">
      📊 Health
    </button>
  </nav>
  <div class="header-end">
    <button class="undo-btn" id="undo-btn" onclick="undoLastDelete()" aria-label="Undo last deletion" title="Restore the last deleted job">
      ↩ Undo
    </button>
    <div class="last-updated" id="last-updated" aria-live="polite">Last updated —</div>
  </div>
</header>

<main id="app" role="tabpanel" aria-label="Dashboard content">
  <div class="loading">
    <div class="spinner" aria-hidden="true"></div>
    Loading your job board…
  </div>
</main>

<!-- Reject confirm overlay -->
<div class="overlay" id="reject-overlay" role="dialog" aria-labelledby="reject-dialog-title" aria-modal="true">
  <div class="confirm-box">
    <h3 id="reject-dialog-title">Done with this one?</h3>
    <p>It'll disappear from your board. Hit Undo in the header if you change your mind.</p>
    <div class="confirm-actions">
      <button class="btn" onclick="closeRejectOverlay()">Keep it</button>
      <button class="btn btn-confirm-reject" onclick="confirmReject()">Remove it</button>
    </div>
  </div>
</div>

<!-- Toast — contains message span + optional undo button -->
<div class="toast" id="toast">
  <span id="toast-msg"></span>
</div>

<script>
// ── State ──────────────────────────────────────────────────────────────────────
let boardData       = null;
let skippedData     = null;
let pendingRejectId = null;
let lastRejectedId  = null;  // tracks last deleted job for persistent undo
let currentTab      = 'board';
const COLUMNS    = ['Reviewing', 'Applied', 'Interviewing'];


const boardFilters = {
  search: '',
  tier: 'all',
  flagged: 'all',
  sort: 'fit_asc',
};

function setBoardFilter(key, value) {
  boardFilters[key] = value;
  if (boardData) renderBoard(boardData);
}

let _searchTimer = null;
function debouncedSearch(value) {
  clearTimeout(_searchTimer);
  _searchTimer = setTimeout(() => setBoardFilter('search', value), 180);
}

function applyBoardFilters(data) {
  const filtered = { columns: {} };
  for (const col of COLUMNS) {
    let cards = [...(data.columns[col] || [])];

    if (boardFilters.search) {
      const needle = boardFilters.search.trim().toLowerCase();
      cards = cards.filter(job => [job.title, job.company, job.location, job.reason, job.source]
        .filter(Boolean)
        .some(v => String(v).toLowerCase().includes(needle)));
    }

    if (boardFilters.tier !== 'all') {
      cards = cards.filter(job => job.tier === boardFilters.tier);
    }

    if (boardFilters.flagged === 'flagged') {
      cards = cards.filter(job => !!job.flagged);
    } else if (boardFilters.flagged === 'unflagged') {
      cards = cards.filter(job => !job.flagged);
    }

    cards.sort((a, b) => compareJobs(a, b, boardFilters.sort));
    filtered.columns[col] = cards;
  }
  return filtered;
}

function compareJobs(a, b, mode) {
  if (mode === 'company') {
    return String(a.company || '').localeCompare(String(b.company || ''));
  }
  if (mode === 'salary') {
    return extractSalaryRank(b) - extractSalaryRank(a);
  }
  if (mode === 'fit_asc' || mode === 'fit_desc') {
    const tierOrder = {'Perfect Fit': 0, 'Good Fit': 1, 'Worth a Look': 2, 'Skip': 3};
    const aRank = tierOrder[a.tier] ?? 9;
    const bRank = tierOrder[b.tier] ?? 9;
    const diff = mode === 'fit_asc' ? aRank - bRank : bRank - aRank;
    // Tiebreak by date (newest first) so same-tier cards stay in a consistent order
    if (diff !== 0) return diff;
    return parseDateForSort(b.date_found) - parseDateForSort(a.date_found);
  }
  return parseDateForSort(b.date_found) - parseDateForSort(a.date_found);
}

function extractSalaryRank(job) {
  const raw = String(job.salary_display || '').replace(/,/g, '');
  const nums = raw.match(/\d+(?:\.\d+)?/g);
  if (!nums || !nums.length) return -1;
  return Math.max(...nums.map(Number));
}

function parseDateForSort(value) {
  if (!value) return 0;
  const parsed = Date.parse(value);
  if (!Number.isNaN(parsed)) return parsed;
  const rel = String(value).match(/(\d+)\s+(day|days|hour|hours|minute|minutes|week|weeks)/i);
  if (!rel) return 0;
  const amount = Number(rel[1]);
  const unit = rel[2].toLowerCase();
  const now = Date.now();
  const mult = unit.startsWith('minute') ? 60 * 1000
    : unit.startsWith('hour') ? 60 * 60 * 1000
    : unit.startsWith('week') ? 7 * 24 * 60 * 60 * 1000
    : 24 * 60 * 60 * 1000;
  return now - (amount * mult);
}


// ── Tab switching ──────────────────────────────────────────────────────────────
function showTab(tab) {
  currentTab = tab;
  document.getElementById('tab-board').classList.toggle('active', tab === 'board');
  document.getElementById('tab-skipped').classList.toggle('active', tab === 'skipped');
  document.getElementById('tab-health').classList.toggle('active', tab === 'health');
  // Update ARIA selected state
  document.getElementById('tab-board').setAttribute('aria-selected', tab === 'board');
  document.getElementById('tab-skipped').setAttribute('aria-selected', tab === 'skipped');
  document.getElementById('tab-health').setAttribute('aria-selected', tab === 'health');
  // Update URL hash for tab persistence across refreshes
  history.replaceState(null, '', '#' + tab);
  if (tab === 'board') {
    loadBoard();
  } else if (tab === 'health') {
    loadHealth();
  } else {
    loadSkipped();
  }
}

// ── Board tab ──────────────────────────────────────────────────────────────────
async function loadBoard() {
  try {
    const res  = await fetch('/api/board');
    const data = await res.json();
    boardData  = data;
    renderBoard(data);
    updateStats(data);
    document.getElementById('last-updated').textContent = 'Last updated ' + data.last_updated;
  } catch (e) {
    document.getElementById('app').innerHTML =
      '<div class="loading" style="color:#e05a5a">Failed to load board. <button class="btn" onclick="loadBoard()" style="margin-left:12px">Retry</button></div>';
  }
}

// ── Skipped tab ────────────────────────────────────────────────────────────────
async function loadSkipped() {
  document.getElementById('app').innerHTML =
    '<div class="loading"><div class="spinner"></div>Loading skipped jobs…</div>';
  try {
    const res  = await fetch('/api/skipped');
    const data = await res.json();
    skippedData = data;
    renderSkipped(data);
    document.getElementById('last-updated').textContent = 'Last updated ' + data.last_updated;
    document.getElementById('badge-skipped').textContent = data.total;
    document.getElementById('count-skipped').textContent = data.total;
  } catch(e) {
    document.getElementById('app').innerHTML =
      '<div class="loading" style="color:#e05a5a">Failed to load skipped jobs. <button class="btn" onclick="loadSkipped()" style="margin-left:12px">Retry</button></div>';
  }
}

function renderSkipped(data) {
  const app = document.getElementById('app');
  if (!data.jobs || data.jobs.length === 0) {
    app.innerHTML = `
      <div class="dashboard-shell">
        <div class="skipped-view">
          <div class="skip-shell">
            <div class="skipped-header">
              <div>
                <div class="eyebrow" style="margin-bottom:12px;background:rgba(245,158,11,.12);border-color:rgba(245,158,11,.28);color:#f6c56f;">Filtered out</div>
                <div class="skipped-title">Skipped Jobs</div>
                <div class="skipped-subtitle">Jobs your autopilot decided weren't a good fit</div>
              </div>
            </div>
            <div class="empty-state">Nothing filtered out yet. Check back after the next run.</div>
          </div>
        </div>
      </div>`;
    return;
  }

  let html = `
    <div class="dashboard-shell">
      <div class="skipped-view">
        <div class="skip-shell">
          <div class="skipped-header">
            <div>
              <div class="eyebrow" style="margin-bottom:12px;background:rgba(245,158,11,.12);border-color:rgba(245,158,11,.28);color:#f6c56f;">Filtered out</div>
              <div class="skipped-title">Skipped Jobs</div>
              <div class="skipped-subtitle">${data.total} job${data.total !== 1 ? 's' : ''} your autopilot decided weren't a good fit — catch anything worth applying for and delete the rest</div>
            </div>
          </div>
          <div class="skipped-list">`;

  _skipJobMap.clear();
  for (const job of data.jobs) {
    const cardId    = job.id.replace(/[^a-zA-Z0-9]/g, '-');
    const isFlagged = job.flagged;
    _skipJobMap.set(cardId, job);
    const salaryPill = job.salary_display ? `<span class="meta-pill salary-pill">💰 ${escHtml(job.salary_display)}</span>` : '';
    const sourcePill = job.source ? `<span class="meta-pill">${escHtml(job.source)}</span>` : '';
    const datePill = job.date_found ? `<span class="meta-pill">${escHtml(job.date_found)}</span>` : '';

    html += `
      <div class="skip-card ${isFlagged ? 'flagged-card' : ''}" id="skipcard-${cardId}" data-job-id="${escHtml(job.id)}">
        <div class="skip-card-top">
          <div class="skip-card-info">
            <div class="skip-card-title">${escHtml(job.title)}</div>
            <div class="skip-card-company">${escHtml(job.company)}${job.location ? ' · ' + escHtml(job.location) : ''}</div>
            <div class="skip-meta-row">
              ${salaryPill}
              ${sourcePill}
              ${datePill}
              ${isFlagged ? '<span class="meta-pill flagged-pill">🚩 Flagged for review</span>' : ''}
            </div>
          </div>
        </div>
        <div class="skip-reason">${escHtml(humanizeSkipReason(job.reason) || 'No reason recorded.')}</div>
        <div class="skip-actions">
          ${job.url
            ? `<button class="btn btn-link" data-url="${escHtml(job.url)}" onclick="openLinkSafe(this)">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                View job</button>`
            : ''}
          <button class="btn btn-success" onclick="restoreJob(this, '${cardId}')">✓ Add to Command Center</button>
          <button class="btn ${isFlagged ? 'btn-flag flagged' : 'btn-flag'}" onclick="toggleFlagPanel('${cardId}')">🚩 ${isFlagged ? 'Flagged' : 'Flag an Issue'}</button>
          <button class="btn btn-danger" onclick="dismissSkip(this, '${cardId}')" title="Hide permanently" aria-label="Dismiss this job permanently">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4a1 1 0 011-1h4a1 1 0 011 1v2"/></svg>
          </button>
        </div>
        <div class="flag-panel ${isFlagged ? 'open' : ''}" id="flagpanel-${cardId}">
          <div class="flag-panel-label">🚩 Why should this job have been included?</div>
          <textarea class="flag-note-area" id="flagnote-${cardId}" placeholder="Tell the autopilot why — e.g. 'This is servicing, not underwriting'"></textarea>
          <div class="flag-panel-actions">
            <button class="btn btn-flag" onclick="submitFlag(this, '${cardId}')">Submit Flag</button>
            <button class="btn" onclick="toggleFlagPanel('${cardId}')">Cancel</button>
          </div>
        </div>
      </div>`;
  }

  html += '</div></div></div></div>';
  app.innerHTML = html;
}

// Module-level map: cardId -> job object. Populated by renderSkipped().
// Avoids embedding serialised JSON in onclick attributes (Fix 3).
const _skipJobMap = new Map();

// ── Health tab ─────────────────────────────────────────────────────────────────
async function loadHealth() {
  const app = document.getElementById('app');
  app.innerHTML = '<div class="loading"><div class="spinner"></div>Loading run history…</div>';
  try {
    const res  = await fetch('/api/health');
    const data = await res.json();
    renderHealth(data);
    // Load query/source performance stats after main render
    loadQueryStats();
  } catch (e) {
    app.innerHTML = '<div class="loading" style="color:#e05a5a">Failed to load health data. <button class="btn" onclick="loadHealth()" style="margin-left:12px">Retry</button></div>';
  }
}

async function loadQueryStats() {
  const panel = document.getElementById('health-query-stats-panel');
  if (!panel) return;
  try {
    const res  = await fetch('/api/health/query-stats');
    const data = await res.json();
    if (data.note) {
      panel.innerHTML = '<span style="color:var(--muted);font-size:13px">' + escHtml(data.note) + '</span>';
      return;
    }
    const sources = data.sources || [];
    if (!sources.length) {
      panel.innerHTML = '<span style="color:var(--muted);font-size:13px">No data yet</span>';
      return;
    }
    const rows = sources.map(s => {
      const pct = s.filter_rate || 0;
      const barClass = pct >= 80 ? 'hq-bar high' : 'hq-bar';
      return '<tr>'
        + '<td>' + escHtml(s.source) + '</td>'
        + '<td class="hq-num">' + s.raw + '</td>'
        + '<td class="hq-num">' + s.new + '</td>'
        + '<td class="hq-num">' + s.filtered + '</td>'
        + '<td><span class="hq-rate">' + pct + '%</span>'
        +   '<span class="hq-bar-wrap"><span class="' + barClass + '" style="width:' + Math.min(pct, 100) + '%"></span></span>'
        + '</td>'
        + '</tr>';
    }).join('');
    panel.innerHTML =
      '<div class="health-table-wrap">'
      + '<table class="health-query-table">'
      + '<thead><tr>'
      + '<th>Source</th>'
      + '<th class="hq-num">Raw</th>'
      + '<th class="hq-num">New</th>'
      + '<th class="hq-num">Filtered</th>'
      + '<th>Filter rate</th>'
      + '</tr></thead>'
      + '<tbody>' + rows + '</tbody>'
      + '</table></div>';
  } catch (e) {
    if (panel) panel.innerHTML = '<span style="color:var(--muted);font-size:13px">Could not load source stats.</span>';
  }
}

// ── Shared filter label map (used by renderHealth + loadFilterDrilldown) ──────
const FILTER_LABELS = {
  'below_salary_floor':         'Below salary floor',
  'category_page':              'Category page (not a job)',
  'empty_company':              'Missing company name',
  'wrong_title':                'Wrong job title',
  'hard_disqualifier':          'Hard disqualifier',
  'company_prefilter':          'Blocked company',
  'seen':                       'Already seen',
  'no_description':             'No description',
  'location_mismatch':          'Location mismatch',
  'onsite_outside_local_metro': 'On-site outside local metro',
  'non_us_location':            'Non-US location',
  'bad_scrape':                 'Bad scrape / malformed',
  'expired_posting':            'Expired / closed posting',
};

function fmtRunDate(iso) {
  const d = new Date(iso);
  const mon = d.toLocaleString('en-US', { month: 'short' });
  const day = d.getDate();
  const hh  = String(d.getHours()).padStart(2, '0');
  const mm  = String(d.getMinutes()).padStart(2, '0');
  return `${mon} ${day} ${hh}:${mm}`;
}

function renderHealth(data) {
  const app = document.getElementById('app');
  if (!data.runs || data.runs.length === 0) {
    app.innerHTML = `
      <div class="dashboard-shell">
        <div class="health-view">
          <div class="health-hero">
            <div class="health-header">
              <div class="eyebrow" style="margin-bottom:12px;">Job Autopilot Health</div>
              <div class="health-title">Autopilot Functionality</div>
              <div class="health-subtitle">Statistics for every run, every source, every filter — all in one place</div>
            </div>
            <div class="health-no-data">Nothing here yet. It will populate after your next autopilot run.</div>
          </div>
        </div>
      </div>`;
    return;
  }

  const sources = data.sources || [];
  const latest = data.runs[0] || {};
  // Summary stats calculated from the last 30 runs (all runs returned by the API)
  const avgDuration = Math.round(data.runs.reduce((sum, run) => {
    if (!run.started_at || !run.finished_at) return sum;
    const delta = (new Date(run.finished_at) - new Date(run.started_at)) / 1000;
    return sum + (Number.isFinite(delta) ? delta : 0);
  }, 0) / Math.max(1, data.runs.filter(r => r.started_at && r.finished_at).length));
  const totalRaw = data.runs.reduce((sum, run) => sum + (run.total_raw || 0), 0);
  const totalNew = data.runs.reduce((sum, run) => sum + (run.total_new || 0), 0);

  // Build rows without per-source columns to avoid horizontal scrolling.
  // Top sources for a run are shown as a compact comma-separated list in a tooltip.
  const rows = data.runs.map(run => {
    const date = run.started_at ? fmtRunDate(run.started_at) : '—';
    const dur = (run.started_at && run.finished_at)
      ? Math.round((new Date(run.finished_at) - new Date(run.started_at)) / 1000) + 's'
      : '—';
    // Top 3 sources by raw count for the tooltip
    const topSources = (run.sources || [])
      .sort((a, b) => (b.raw_count || 0) - (a.raw_count || 0))
      .slice(0, 3)
      .map(s => `${s.source}: ${s.raw_count}`)
      .join(', ') || '—';
    const filterSummary = (run.filters || []).map(f => `${f.reason}: ${f.count}`).join(' | ') || 'none';
    return `<tr title="Sources: ${escHtml(topSources)} | Filtered: ${escHtml(filterSummary)}">
      <td>${escHtml(date)}</td>
      <td class="health-num">${run.total_raw || 0}</td>
      <td class="health-num">${run.total_new || 0}</td>
      <td class="health-num">${run.total_rated || 0}</td>
      <td>${escHtml(dur)}</td>
    </tr>`;
  }).join('');

  // Build per-source-per-run table: one row per run, one column per source showing raw_count.
  // Collect all source names seen across all runs, sorted alphabetically.
  const allSourceNames = [...new Set(
    data.runs.flatMap(run => (run.sources || []).map(s => s.source))
  )].sort();

  const sourceRows = data.runs.map(run => {
    const date = run.started_at ? fmtRunDate(run.started_at) : '—';
    // Build a lookup map for this run's sources
    const srcMap = {};
    (run.sources || []).forEach(s => { srcMap[s.source] = s.raw_count || 0; });
    const cells = allSourceNames.map(name => {
      const count = srcMap[name] || 0;
      return `<td class="health-num ${count === 0 ? 'health-zero' : ''}">${count === 0 ? '—' : count}</td>`;
    }).join('');
    return `<tr><td>${escHtml(date)}</td>${cells}</tr>`;
  }).join('');

  const sourceHeaderCells = allSourceNames.map(name =>
    `<th class="health-num">${escHtml(name)}</th>`
  ).join('');

  const filterTotals = {};
  data.runs.forEach(run => {
    (run.filters || []).forEach(f => {
      filterTotals[f.reason] = (filterTotals[f.reason] || 0) + f.count;
    });
  });
  const filterPills = Object.entries(filterTotals)
    .sort((a, b) => b[1] - a[1])
    .map(([r, c]) => {
      const label = FILTER_LABELS[r] || r.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
      return `<div class="health-filter-pill" onclick="loadFilterDrilldown('${escHtml(r)}', this)" data-reason="${escHtml(r)}"><strong>${c}</strong> ${escHtml(label)}</div>`;
    })
    .join('');

  app.innerHTML = `
    <div class="dashboard-shell">
      <div class="health-view">
        <div class="health-hero">
          <div class="health-header">
            <div class="eyebrow" style="margin-bottom:12px;">Job Autopilot Health</div>
            <div class="health-title">Autopilot Functionality</div>
            <div class="health-subtitle">Your last ${data.runs.length} runs</div>
          </div>
          <div class="health-summary-grid">
            <div class="health-summary-card">
              <div class="health-summary-label">Latest run</div>
              <div class="health-summary-value">${latest.total_new || 0}</div>
              <div class="health-summary-foot">new jobs out of ${latest.total_raw || 0} scanned</div>
            </div>
            <div class="health-summary-card">
              <div class="health-summary-label">Average duration</div>
              <div class="health-summary-value">${Number.isFinite(avgDuration) ? avgDuration : 0}s</div>
              <div class="health-summary-foot">based on completed runs</div>
            </div>
            <div class="health-summary-card">
              <div class="health-summary-label">Sources tracked</div>
              <div class="health-summary-value">${sources.length}</div>
              <div class="health-summary-foot">job sources active</div>
            </div>
            <div class="health-summary-card">
              <div class="health-summary-label">Total volume</div>
              <div class="health-summary-value">${totalNew}</div>
              <div class="health-summary-foot">new jobs found across all runs</div>
            </div>
          </div>
        </div>

        <div class="health-panel">
          <div class="health-section-title">Run history</div>
          <div class="health-table-wrap">
            <table class="health-table">
              <thead>
                <tr>
                  <th>Run</th>
                  <th class="health-num">Raw</th>
                  <th class="health-num">New</th>
                  <th class="health-num">Rated</th>
                  <th>Duration</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>
          </div>

          <div class="health-section-title">Jobs found per source</div>
          <div class="health-table-wrap" style="overflow-x: auto; position: relative;">
            <table class="health-table">
              <thead>
                <tr>
                  <th>Run</th>
                  ${sourceHeaderCells}
                </tr>
              </thead>
              <tbody>${sourceRows}</tbody>
            </table>
          </div>

          <div class="health-section-title">What got filtered out</div>
          <div class="health-filter-grid">${filterPills || '<span style="color:var(--muted);font-size:13px">Nothing filtered yet</span>'}</div>
          <div id="health-drilldown-panel"></div>

          <div class="health-section-title">How each source is performing</div>
          <div id="health-query-stats-panel"><span style="color:var(--muted);font-size:13px">Loading…</span></div>
        </div>
      </div>
    </div>`;
}

// ── Health filter drilldown ────────────────────────────────────────────────────
let _activeDrilldownReason = null;

async function loadFilterDrilldown(reason, pillEl) {
  const panel = document.getElementById('health-drilldown-panel');
  if (!panel) return;

  // Toggle off if same pill clicked again
  if (_activeDrilldownReason === reason) {
    _activeDrilldownReason = null;
    panel.innerHTML = '';
    document.querySelectorAll('.health-filter-pill').forEach(p => p.classList.remove('active'));
    return;
  }

  _activeDrilldownReason = reason;
  document.querySelectorAll('.health-filter-pill').forEach(p => p.classList.remove('active'));
  if (pillEl) pillEl.classList.add('active');

  const label = FILTER_LABELS[reason] || reason.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());

  panel.innerHTML = '<div class="health-drilldown"><div class="health-drilldown-empty">Loading\u2026</div></div>';

  try {
    const res  = await fetch('/api/health/filter/' + encodeURIComponent(reason));
    const data = await res.json();

    if (data.note) {
      panel.innerHTML = '<div class="health-drilldown"><div class="health-drilldown-empty">' + escHtml(data.note) + '</div></div>';
      return;
    }

    const jobs = data.jobs || [];
    const items = jobs.length === 0
      ? '<div class="health-drilldown-empty">No jobs recorded for this filter yet.</div>'
      : jobs.map(j => {
          const title   = escHtml(j.title   || '(no title)');
          const company = escHtml(j.company || '');
          const source  = escHtml(j.source  || '');
          const linked  = j.url
            ? '<a href="' + escHtml(j.url) + '" target="_blank" rel="noopener">' + title + '</a>'
            : title;
          return '<div class="health-drilldown-item">'
            + '<span class="health-drilldown-title">'   + linked   + '</span>'
            + '<span class="health-drilldown-company">' + company  + '</span>'
            + '<span class="health-drilldown-source">'  + source   + '</span>'
            + '</div>';
        }).join('');

    panel.innerHTML =
      '<div class="health-drilldown">'
      + '<div class="health-drilldown-header">'
      +   '<span>' + escHtml(label) + ' &mdash; ' + jobs.length + ' job' + (jobs.length !== 1 ? 's' : '') + '</span>'
      +   '<button class="health-drilldown-close" data-reason="' + escHtml(reason) + '" onclick="loadFilterDrilldown(this.dataset.reason, null)" title="Close">\u2715</button>'
      + '</div>'
      + '<div class="health-drilldown-list">' + items + '</div>'
      + '</div>';
  } catch (e) {
    panel.innerHTML = '<div class="health-drilldown"><div class="health-drilldown-empty">Failed to load filter details.</div></div>';
  }
}

// Fix 2: Safe URL opener that reads from data-url attribute, never from inline string.
function openLinkSafe(btn) {
  const url = btn.dataset.url;
  if (url) window.open(url, '_blank', 'noopener');
}

// Toggle the flag note panel on skipped tab cards.
// Separate from toggleBoardFlagPanel to keep skipped and board logic independent.
function toggleFlagPanel(cardId) {
  const panel = document.getElementById('flagpanel-' + cardId);
  if (panel) panel.classList.toggle('open');
}

// ── Skipped actions ────────────────────────────────────────────────────────────
async function restoreJob(btn, cardId) {
  const card = document.getElementById('skipcard-' + cardId);
  const id   = card ? card.dataset.jobId : '';
  if (!id) { console.error('restoreJob: no job id for cardId', cardId); return; }
  try {
    const resp = await fetch('/api/restore', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id}),
    });
    // FIX (code review): only remove card from DOM after server confirms success.
    // Previously the card was removed regardless of resp.ok, so a 500 would leave
    // the job disappearing from Skipped but never appearing in Reviewing.
    if (!resp.ok) {
      showToast('Error restoring job — try again');
      return;
    }
  } catch (e) {
    console.error('restoreJob network error:', e);
    showToast('Network error — is the dashboard running?');
    return;
  }
  card?.remove();
  _skipJobMap.delete(cardId);
  updateSkippedBadge(-1);
  showToast('Moved to your board');
}

async function dismissSkip(btn, cardId) {
  const card = document.getElementById('skipcard-' + cardId);
  const id   = card ? card.dataset.jobId : '';
  if (!id) { console.error('dismissSkip: no job id for cardId', cardId); return; }
  try {
    const resp = await fetch('/api/skip_dismiss', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id}),
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      console.error('dismissSkip failed:', resp.status, err);
      showToast('Error hiding job — check console');
      return;
    }
  } catch (e) {
    console.error('dismissSkip network error:', e);
    showToast('Network error — is the dashboard running?');
    return;
  }
  document.getElementById('skipcard-' + cardId)?.remove();
  _skipJobMap.delete(cardId);
  updateSkippedBadge(-1);
  showToast('Removed');
  // FIX: if that was the last card, show the empty state so the view
  // doesn't go blank. Check the skipped-list container for remaining cards.
  const list = document.querySelector('.skipped-list');
  if (list && list.querySelectorAll('.skip-card').length === 0) {
    document.getElementById('app').innerHTML = `
      <div class="skipped-view">
        <div class="skipped-header">
          <div>
            <div class="skipped-title">Skipped Jobs</div>
            <div class="skipped-subtitle">Jobs your autopilot decided weren't a good fit</div>
          </div>
        </div>
        <div class="empty-state">Nothing filtered out yet. Check back after the next run.</div>
      </div>`;
  }
}

// Fix 3: Job data is read from _skipJobMap — no JSON serialisation in HTML attributes.
async function submitFlag(btn, cardId) {
  const card = document.getElementById('skipcard-' + cardId);
  const id   = card ? card.dataset.jobId : '';
  if (!id) { console.error('submitFlag: no job id for cardId', cardId); return; }
  const job    = _skipJobMap.get(cardId) || {};
  const noteEl = document.getElementById('flagnote-' + cardId);
  const note   = noteEl ? noteEl.value.trim() : '';
  await fetch('/api/flag', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id, note, job}),
  });
  // Reuse the card reference already declared above — do not redeclare with const
  if (card) {
    card.classList.add('flagged-card');
    const panel = document.getElementById('flagpanel-' + cardId);
    if (panel) panel.classList.remove('open');
  }
  showToast('🚩 Flagged for review — saved to flagged_for_review.json');
}

// Fix 9: Adjust skipped badge count without a full reload.
function updateSkippedBadge(delta) {
  const badge = document.getElementById('badge-skipped');
  const stat  = document.getElementById('count-skipped');
  const curr  = parseInt(badge?.textContent || '0', 10);
  const next  = Math.max(0, curr + delta);
  if (badge) badge.textContent = next;
  if (stat)  stat.textContent  = next;
}

// ── Board render ───────────────────────────────────────────────────────────────
function renderBoard(data) {
  const board = document.getElementById('app');
  _boardJobMap.clear();

  const filteredData = applyBoardFilters(data);
  const counts = {
    reviewing: (data.columns['Reviewing'] || []).length,
    applied: (data.columns['Applied'] || []).length,
    interviewing: (data.columns['Interviewing'] || []).length,
    skipped: Number(document.getElementById('count-skipped')?.textContent || 0),
  };

  let html = `
    <div class="dashboard-shell">
      <section class="hero">
        <div class="hero-top">
          <div class="hero-copy">
            <div class="eyebrow">Daily job command center</div>
            <div class="hero-title">Job Search Autopilot</div>
            <div class="hero-subtitle">Track fresh opportunities, and move good jobs through your pipeline.</div>
          </div>
          <div class="hero-timestamp">
            Dashboard Last Updated
            <strong>${escHtml(data.last_updated || 'Just now')}</strong>
          </div>
        </div>

        <div class="kpi-grid">
          <div class="kpi-card" style="--kpi-accent: var(--accent)">
            <div class="kpi-label">Reviewing</div>
            <div class="kpi-value">${counts.reviewing}</div>
            <div class="kpi-foot">Fresh jobs waiting for a decision</div>
          </div>
          <div class="kpi-card" style="--kpi-accent: var(--good)">
            <div class="kpi-label">Applied</div>
            <div class="kpi-value">${counts.applied}</div>
            <div class="kpi-foot">Jobs you've applied for</div>
          </div>
          <div class="kpi-card" style="--kpi-accent: var(--interview)">
            <div class="kpi-label">Interviewing</div>
            <div class="kpi-value">${counts.interviewing}</div>
            <div class="kpi-foot">Interviews in progress</div>
          </div>
          <div class="kpi-card" style="--kpi-accent: var(--skipped)">
            <div class="kpi-label">Skipped</div>
            <div class="kpi-value">${counts.skipped}</div>
            <div class="kpi-foot">Skipped based on your filters</div>
          </div>
        </div>

        <div class="board-toolbar">
          <input class="toolbar-field" id="board-search" type="text" placeholder="Search by title, company, or description" value="${escHtml(boardFilters.search)}" oninput="debouncedSearch(this.value)">
          <select class="toolbar-select" id="board-tier" onchange="setBoardFilter('tier', this.value)">
            <option value="all" ${boardFilters.tier === 'all' ? 'selected' : ''}>Tier: All</option>
            <option value="Perfect Fit" ${boardFilters.tier === 'Perfect Fit' ? 'selected' : ''}>Tier: Excellent Fit</option>
            <option value="Good Fit" ${boardFilters.tier === 'Good Fit' ? 'selected' : ''}>Tier: Good Fit</option>
            <option value="Worth a Look" ${boardFilters.tier === 'Worth a Look' ? 'selected' : ''}>Tier: Worth a Look</option>
          </select>
          <select class="toolbar-select" id="board-flagged" onchange="setBoardFilter('flagged', this.value)">
            <option value="all" ${boardFilters.flagged === 'all' ? 'selected' : ''}>Flagged: All</option>
            <option value="flagged" ${boardFilters.flagged === 'flagged' ? 'selected' : ''}>Flagged only</option>
            <option value="unflagged" ${boardFilters.flagged === 'unflagged' ? 'selected' : ''}>Unflagged only</option>
          </select>
          <select class="toolbar-select" id="board-sort" onchange="setBoardFilter('sort', this.value)">
            <option value="newest" ${boardFilters.sort === 'newest' ? 'selected' : ''}>Sort: Newest</option>
            <option value="fit_asc" ${boardFilters.sort === 'fit_asc' ? 'selected' : ''}>Best match first</option>
            <option value="fit_desc" ${boardFilters.sort === 'fit_desc' ? 'selected' : ''}>Weakest match first</option>
            <option value="salary" ${boardFilters.sort === 'salary' ? 'selected' : ''}>Sort: Highest salary</option>
            <option value="company" ${boardFilters.sort === 'company' ? 'selected' : ''}>Sort: Company A–Z</option>
          </select>
        </div>
      </section>

      <div class="board">`;

  const colMeta = {
    'Reviewing':    { accent: 'reviewing-accent',     label: 'Reviewing' },
    'Applied':      { accent: 'applied-accent',       label: 'Applied' },
    'Interviewing': { accent: 'interviewing-accent',  label: 'Interviewing' },
  };

  for (const col of COLUMNS) {
    const cards = filteredData.columns[col] || [];
    const meta  = colMeta[col];

    html += `
      <div class="column" id="col-${col.toLowerCase()}">
        <div class="col-header">
          <div class="col-title">
            <span class="col-accent ${meta.accent}"></span>
            ${meta.label}
          </div>
          <span class="col-count">${cards.length}</span>
        </div>
        <div class="cards" id="cards-${col}">`;

    if (cards.length === 0) {
      html += `<div class="empty-state">Nothing here yet</div>`;
    } else {
      for (const job of cards) {
        html += renderCard(job, col);
      }
    }

    html += `</div></div>`;
  }

  html += '</div></div>';
  board.innerHTML = html;
}

function tierClass(tier) {
  if (tier === 'Perfect Fit') return 'tier-perfect';
  if (tier === 'Good Fit')    return 'tier-good';
  return 'tier-look';
}

function tierEmoji(tier) {
  if (tier === 'Perfect Fit') return '⭐';
  if (tier === 'Good Fit')    return '🟢';
  return '🟡';
}

function tierLabel(tier) {
  if (tier === 'Perfect Fit') return 'Excellent Fit';
  return tier;
}

function renderCard(job, col) {
  const tc   = tierClass(job.tier);
  const prev = COLUMNS[COLUMNS.indexOf(col) - 1];
  const next = COLUMNS[COLUMNS.indexOf(col) + 1];

  const salHtml = job.salary_display
    ? `<span class="meta-pill salary-pill">💰 ${escHtml(job.salary_display)}</span>`
    : '';
  const srcHtml  = job.source    ? `<span class="meta-pill">${escHtml(job.source)}</span>`     : '';
  const dateHtml = job.date_found ? `<span class="meta-pill">${escHtml(job.date_found)}</span>` : '';

  const reasonId  = 'reason-' + job.id.replace(/[^a-zA-Z0-9]/g, '-');

  // Store job in map so flag actions can read data safely without inline JSON
  const cardId = job.id.replace(/[^a-zA-Z0-9]/g, '-');
  _boardJobMap.set(cardId, job);

  const isFlagged = job.flagged || false;

  const prevBtn = prev
    ? `<button class="btn" onclick="moveCard('${cardId}','${prev}')" title="Move to ${prev}">← ${prev}</button>`
    : '';

  const nextBtn = next
    ? `<button class="btn btn-${next === 'Applied' ? 'success' : 'orange'}"
         onclick="moveCard('${cardId}','${next}')">
         ${next === 'Applied' ? '✓ Applied' : '📞 Interviewing'}</button>`
    : '';

  const appliedBadge = col === 'Applied' && job.applied_date
    ? `<span class="applied-badge">Applied ${job.applied_date}</span>`
    : '';

  return `
    <div class="card" id="card-${cardId}" data-job-id="${escHtml(job.id)}">
      <div class="card-tier ${tc}">${tierEmoji(job.tier)} ${escHtml(tierLabel(job.tier))}</div>
      <div class="card-title">${escHtml(job.title)}</div>
      <div class="card-company">${escHtml(job.company)}${job.location ? ' · ' + escHtml(job.location) : ''}</div>
      <div class="card-meta">${salHtml}${srcHtml}${dateHtml}${isFlagged ? '<span class="meta-pill" style="color:var(--flagged)">🚩 Flagged</span>' : ''}</div>
      <div class="card-reason" id="${reasonId}" onclick="toggleReason('${reasonId}')">
        ${escHtml(job.reason)}
      </div>
      <div class="card-actions">
        ${job.url
          ? `<button class="btn btn-link" data-url="${escHtml(job.url)}" onclick="openLinkSafe(this)">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
              View job</button>`
          : ''}

        <button class="btn ${isFlagged ? 'btn-flag flagged' : 'btn-flag'}"
          onclick="toggleBoardFlagPanel('${cardId}')" title="Flag this job — tell us why it shouldn't have appeared">
          🚩 ${isFlagged ? 'Flagged' : 'Flag'}
        </button>
        ${prevBtn}
        ${nextBtn}
        ${appliedBadge}
        <button class="btn btn-danger" onclick="askReject('${cardId}')" title="Remove from board" aria-label="Remove this job from board">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4a1 1 0 011-1h4a1 1 0 011 1v2"/></svg>
        </button>
      </div>
      <div class="card-flag-panel ${isFlagged ? 'open' : ''}" id="boardflagpanel-${cardId}">
        <div class="flag-panel-label">🚩 What's wrong with this one?</div>
        <textarea class="flag-note-area" id="boardflagnote-${cardId}"
          placeholder="e.g. 'Staffing agency' or 'Title says PM but it's a sales role'"></textarea>
        <div class="flag-panel-actions">
          <button class="btn btn-flag" onclick="submitBoardFlag('${cardId}')">Submit Flag</button>
          <button class="btn" onclick="toggleBoardFlagPanel('${cardId}')">Cancel</button>
        </div>
      </div>
    </div>`;
}

// Module-level map: cardId -> job object for board cards.
// Populated by renderCard() — avoids embedding JSON in onclick attributes.
// FIX (code review): cleared at the start of each renderBoard() call so stale
// entries from rejected/dismissed jobs don't accumulate across reloads.
const _boardJobMap = new Map();

function toggleBoardFlagPanel(cardId) {
  const panel = document.getElementById('boardflagpanel-' + cardId);
  if (panel) panel.classList.toggle('open');
}

async function submitBoardFlag(cardId) {
  const card = document.getElementById('card-' + cardId);
  const id   = card ? card.dataset.jobId : '';
  if (!id) { console.error('submitBoardFlag: no job id for cardId', cardId); return; }
  const job    = _boardJobMap.get(cardId) || {};
  const noteEl = document.getElementById('boardflagnote-' + cardId);
  const note   = noteEl ? noteEl.value.trim() : '';
  await fetch('/api/flag', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id, note, job}),
  });
  // Mark card as flagged visually without reloading the full board
  if (card) {
    const panel = document.getElementById('boardflagpanel-' + cardId);
    if (panel) panel.classList.remove('open');
    const flagBtn = card.querySelector('.btn-flag');
    if (flagBtn) { flagBtn.classList.add('flagged'); flagBtn.textContent = '🚩 Flagged'; }
    // Add flagged pill to meta row if not already there
    const meta = card.querySelector('.card-meta');
    if (meta && !meta.querySelector('.flag-pill')) {
      const pill = document.createElement('span');
      pill.className = 'meta-pill flag-pill';
      pill.style.color = 'var(--flagged)';
      pill.textContent = '🚩 Flagged';
      meta.appendChild(pill);
    }
  }
  showToast('🚩 Flagged for review — saved to flagged_for_review.json');
}

// ── Board actions ──────────────────────────────────────────────────────────────
async function undoLastDelete() {
  if (!lastRejectedId) return;
  try {
    const resp = await fetch('/api/unreject', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: lastRejectedId}),
    });
    if (!resp.ok) {
      showToast('Error restoring job — try again');
      return;  // keep undo button visible so user can retry
    }
  } catch (e) {
    console.error('undoLastDelete error:', e);
    showToast('Network error — is the dashboard running?');
    return;  // keep undo button visible so user can retry
  }
  lastRejectedId = null;
  document.getElementById('undo-btn').classList.remove('visible');
  loadBoard();
  showToast('Job restored to board');
}
async function moveCard(cardId, column) {
  // cardId is the safe DOM-sanitised key — look up the real job id from the map
  const job = _boardJobMap.get(cardId);
  const id  = job ? job.id : cardId;
  try {
    const resp = await fetch('/api/move', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id, column, job: job || null}),
    });
    if (!resp.ok) { showToast('Error moving card — try again'); return; }
    showToast(`Moved to ${column}`);
    loadBoard();
  } catch (e) {
    console.error('moveCard error:', e);
    showToast('Network error — is the dashboard running?');
  }
}

function askReject(cardId) {
  // cardId is the safe DOM-sanitised key — look up the real job id from the map
  const job = _boardJobMap.get(cardId);
  pendingRejectId = job ? job.id : cardId;
  document.getElementById('reject-overlay').classList.add('visible');
}

function closeRejectOverlay() {
  pendingRejectId = null;
  document.getElementById('reject-overlay').classList.remove('visible');
}

async function confirmReject() {
  if (!pendingRejectId) return;
  const rejectedId = pendingRejectId;
  try {
    const resp = await fetch('/api/reject', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: rejectedId}),
    });
    if (!resp.ok) {
      closeRejectOverlay();
      showToast('Error removing job — try again');
      return;
    }
  } catch (e) {
    console.error('confirmReject error:', e);
    closeRejectOverlay();
    showToast('Network error — is the dashboard running?');
    return;
  }
  closeRejectOverlay();
  // Track the deleted job and show the persistent undo button in the header
  lastRejectedId = rejectedId;
  document.getElementById('undo-btn').classList.add('visible');
  loadBoard();
  showToast('Job removed from board');
}

function toggleReason(reasonId) {
  document.getElementById(reasonId)?.classList.toggle('reason-expanded');
}

// ── Stats ──────────────────────────────────────────────────────────────────────
function updateStats(data) {
  const r = (data.columns['Reviewing']    || []).length;
  const a = (data.columns['Applied']      || []).length;
  const i = (data.columns['Interviewing'] || []).length;
  document.getElementById('count-reviewing').textContent    = r;
  document.getElementById('count-applied').textContent      = a;
  document.getElementById('count-interviewing').textContent = i;
  document.getElementById('badge-board').textContent        = r + a + i;
  // FIX: refresh skipped count from the server on every board load so the
  // header number stays accurate without requiring a tab switch.
  fetch('/api/skipped').then(r => r.json()).then(data => {
    const n = data.total || 0;
    document.getElementById('badge-skipped').textContent = n;
    document.getElementById('count-skipped').textContent = n;
  }).catch(() => {});
}

// ── Utils ──────────────────────────────────────────────────────────────────────

// Translate internal pre-filter reason codes into plain English for the Skipped tab.
function humanizeSkipReason(reason) {
  if (!reason) return reason;

  // Pre-filter pipeline reasons (set before Claude is called)
  const prefilterMap = {
    'category_page':              'Not a real job posting — this was a search results page or job board category page.',
    'bad_scrape':                 'Malformed listing — the job data was incomplete or corrupted.',
    'empty_company':              'No company name found — could not identify the employer.',
    'non_us_location':            'Located outside the US — does not match your location requirements.',
    'onsite_outside_local_metro': 'On-site role outside your local metro area.',
    'below_salary_floor':         'Salary is below your minimum — the posted range does not reach your floor.',
  };
  if (prefilterMap[reason]) return prefilterMap[reason];

  // Wrong title auto-skip
  if (reason === 'Auto-skipped: wrong title level or known staffing agency') {
    return 'Wrong job title — this role does not match your target titles (e.g. software engineer, designer, sales, entry-level).';
  }

  // Hard disqualifier auto-skip — "Auto-disqualified: contains 'X'"
  const disqMatch = reason.match(/^Auto-disqualified: contains '(.+)'$/i);
  if (disqMatch) {
    return `Hard disqualifier — the job contains "${disqMatch[1]}", which is on your never-apply list (crypto, blockchain, staffing agency language, etc.).`;
  }

  // Company pre-filter — "Company pre-filter (staffing): reason text"
  const coMatch = reason.match(/^Company pre-filter \((\w+)\): (.+)$/i);
  if (coMatch) {
    const category = coMatch[1].toLowerCase();
    const detail   = coMatch[2];
    if (category === 'staffing') {
      return `Staffing agency — ${detail}`;
    }
    if (category === 'aggregator') {
      return `Job aggregator — ${detail}`;
    }
    return `Blocked company (${category}) — ${detail}`;
  }

  // Claude-rated Skip — pass through as-is (already human-readable)
  return reason;
}

function escHtml(str) {
  if (!str) return '';
  return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
            .replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}

let toastTimer = null;
function showToast(msg) {
  const t    = document.getElementById('toast');
  const msgEl = document.getElementById('toast-msg');
  msgEl.textContent = msg;
  t.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove('show'), 2800);
}

document.getElementById('reject-overlay').addEventListener('click', function(e) {
  if (e.target === this) closeRejectOverlay();
});

// ── Boot ───────────────────────────────────────────────────────────────────────
// Load skipped count for badge on startup, then show board
(async () => {
  try {
    const res  = await fetch('/api/skipped');
    const data = await res.json();
    // FIX (code review): guard data.total in case API returns unexpected shape
    document.getElementById('badge-skipped').textContent  = data.total || 0;
    document.getElementById('count-skipped').textContent  = data.total || 0;
  } catch(_) {}
})();
// ── Hash-based tab routing ────────────────────────────────────────────────────
const validTabs = ['board', 'skipped', 'health'];
const initialTab = validTabs.includes(location.hash.slice(1)) ? location.hash.slice(1) : 'board';
showTab(initialTab);
window.addEventListener('hashchange', () => {
  const tab = location.hash.slice(1);
  if (validTabs.includes(tab) && tab !== currentTab) showTab(tab);
});
</script>
</body>
</html>"""


def _prune_board_state():
    """Remove board_state entries that are no longer needed.

    IMPORTANT: skip_dismissed entries must NEVER be pruned. They are the
    mechanism that prevents dismissed skipped jobs from reappearing after a
    dashboard restart or after the autopilot writes a new *-skipped.json file.
    Pruning them causes dismissed jobs to come back on every reload — which
    is the bug this function previously introduced.

    The only safe thing to prune here is entries that have NO meaningful state
    at all (empty dicts), which can accumulate from aborted operations.
    """
    state = load_board_state()
    before = len(state)
    # Only remove completely empty entries — nothing meaningful to preserve
    prunable = {jid for jid, s in state.items() if not s}
    for jid in prunable:
        del state[jid]
    pruned = before - len(state)
    if pruned:
        try:
            save_board_state(state)
            print(f"  Board state pruned: removed {pruned} empty entries")
        except OSError:
            pass  # non-critical — pruning is best-effort


if __name__ == "__main__":
    os.makedirs(JOBS_DIR, exist_ok=True)
    _prune_board_state()
    print("\n  Job Search Autopilot Dashboard")
    print("  ─────────────────────────────────────────")
    print("  Open http://localhost:5000 in your browser")
    print("  Press Ctrl+C to stop\n")
    app.run(debug=False, host="127.0.0.1", port=5000, threaded=False)
