from datetime import date
import os

ANCHOR_WEDNESDAY = date.fromisoformat(os.getenv("ANCHOR_WEDNESDAY", "2026-03-04"))
# Example: the first Wednesday that should count as week 1

def second_week(run_date: date) -> bool:
    weeks_since_anchor = (run_date - ANCHOR_WEDNESDAY).days // 7
    return (weeks_since_anchor + 1) % 2 == 0
