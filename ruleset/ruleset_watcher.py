"""
ORACLE Ruleset Watcher — background watcher that periodically runs the watch cycle.

Thin wrapper that registers ruleset_manager.run_watch_cycle() with APScheduler.
"""

from ruleset.ruleset_manager import run_watch_cycle

WATCH_INTERVAL_SECONDS = 60


def start_watcher(scheduler):
    """Register the watch cycle with an APScheduler instance.

    Args:
        scheduler: An APScheduler scheduler (e.g., BackgroundScheduler)
    """
    scheduler.add_job(
        run_watch_cycle,
        "interval",
        seconds=WATCH_INTERVAL_SECONDS,
        id="oracle_ruleset_watcher",
        name="ORACLE Ruleset Watch Cycle",
        replace_existing=True,
    )
