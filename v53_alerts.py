"""Private saved-session changes, evaluated inside the existing publication job."""

import json
import logging

import pandas as pd
from sqlalchemy import text

from db import get_engine
from v5_calibration import utc
from v5_push import quiet_now
from v53_profiles import digest

log = logging.getLogger(__name__)


def plan_metrics(plan):
    night = plan["nights"][0]
    alternative = max(plan["nights"][1:], key=lambda n: n["net_hours"], default={})
    return {
        "targets": {s["target"]: s["continuous_hours"] for s in plan["sessions"]},
        "net_hours": night["net_hours"],
        "alternative_start": alternative.get("start"),
        "alternative_hours": alternative.get("net_hours", 0),
    }


def session_changes(reference, current, rules):
    threshold = rules["minutes"] / 60
    messages = []
    for target, old in reference["targets"].items():
        new = current["targets"].get(target, 0)
        if old - new >= max(threshold, old * rules["percent"] / 100):
            messages.append(
                f"{target}: finestra continua da {old:.1f} a {new:.1f} ore."
            )
    old_advantage = reference["alternative_hours"] - reference["net_hours"]
    advantage = current["alternative_hours"] - current["net_hours"]
    if (
        current["alternative_start"]
        and advantage >= threshold
        and advantage - max(0, old_advantage) >= threshold
    ):
        messages.append(
            f"Un’altra notte offre {current['alternative_hours']:.1f} ore nette, contro {current['net_hours']:.1f} della sessione scelta."
        )
    return messages


def refresh_plan_alerts(now=None):
    from v5_data import clean_json, read_snapshot, station_settings
    from v5_extensions import planner

    now = utc(now if now is not None else pd.Timestamp.now(tz="UTC"))
    candidates = []
    with get_engine().connect() as con:
        states = {
            (r[0], r[1]): json.loads(r[2])
            for r in con.execute(
                text("SELECT account_id,plan_id,payload FROM personal_alerts")
            )
        }
        for account, payload in con.execute(
            text("SELECT account_id,payload FROM personal_profiles")
        ):
            for plan in json.loads(payload).get("plans", []):
                if not plan["alerts"]["enabled"]:
                    continue
                state = states.get((account, plan["id"]), {})
                candidates.append((state.get("evaluated_at", ""), account, plan, state))
    # Bound optional work; old pending plans go first. Public weather keeps cadence.
    for _, account, saved, previous in sorted(candidates, key=lambda v: v[0])[:24]:
        try:
            config = saved["config"]
            station = config["station_id"]
            cfg = station_settings(station)
            start = pd.Timestamp(config["start"])
            start = (
                start.tz_localize(
                    cfg.local_timezone, ambiguous="raise", nonexistent="raise"
                )
                if start.tzinfo is None
                else start
            )
            if start <= now or start > now + pd.Timedelta(days=7):
                with get_engine().begin() as con:
                    con.execute(
                        text(
                            "DELETE FROM personal_alerts WHERE account_id=:id AND plan_id=:plan"
                        ),
                        {"id": account, "plan": saved["id"]},
                    )
                continue
            snapshot = read_snapshot(station)
            generated = utc(snapshot.get("generated_at"))
            if pd.isna(generated) or not pd.Timedelta(
                0
            ) <= now - generated <= pd.Timedelta(minutes=30):
                continue
            issued = [utc(r.get("issued_at")) for r in snapshot.get("forecast", [])]
            if not issued or not any(
                pd.notna(i) and pd.Timedelta(0) <= now - i <= pd.Timedelta(hours=12)
                for i in issued
            ):
                continue
            current_plan = planner(station, config)
            # Missing meteorology is not evidence that an observed or modelled
            # photographic window has deteriorated.
            if any(
                "Dati meteo incompleti" in r.get("reasons", [])
                for r in current_plan.get("session_detail", [])
            ):
                continue
            metrics = clean_json(plan_metrics(current_plan))
            fingerprint = digest(json.dumps(saved, sort_keys=True))
            same = previous.get("plan_hash") == fingerprint
            reference = previous["reference"] if same else metrics
            changes = session_changes(reference, metrics, saved["alerts"])
            history = previous.get("history", []) if same else []
            pending = previous.get("pending") if same else None
            # Quiet time suppresses delivery; compare again with fresh data after it.
            if changes and not quiet_now(saved["alerts"], now, cfg.local_timezone):
                if not pending or pending.get("body") != " ".join(changes):
                    pending = {
                        "kind": "session:" + saved["id"],
                        "event_key": now.isoformat(),
                        "title": "Meteo Pro · " + saved["name"],
                        "body": " ".join(changes),
                        "station": station,
                        "page": "personal",
                        "created_at": now.isoformat(),
                    }
                    history.append({"at": now.isoformat(), "body": pending["body"]})
                reference = metrics
            elif (
                quiet_now(saved["alerts"], now, cfg.local_timezone)
                or pending
                and now - utc(pending["created_at"]) > pd.Timedelta(hours=1)
            ):
                pending = None
            state = {
                "plan_id": saved["id"],
                "plan_hash": fingerprint,
                "name": saved["name"],
                "evaluated_at": now.isoformat(),
                "start": start.isoformat(),
                "reference": reference,
                "current": metrics,
                "pending": pending,
                "history": history[-30:],
                "rules": saved["alerts"],
                "timezone": cfg.local_timezone,
            }
            with get_engine().begin() as con:
                lock = " FOR UPDATE" if con.dialect.name == "postgresql" else ""
                current_profile = con.execute(
                    text(
                        "SELECT payload FROM personal_profiles WHERE account_id=:id"
                        + lock
                    ),
                    {"id": account},
                ).scalar()
                latest = next(
                    (
                        p
                        for p in json.loads(current_profile or "{}").get("plans", [])
                        if p["id"] == saved["id"]
                    ),
                    None,
                )
                if (
                    not latest
                    or digest(json.dumps(latest, sort_keys=True)) != fingerprint
                ):
                    continue
                con.execute(
                    text(
                        "INSERT INTO personal_alerts(account_id,plan_id,payload,evaluated_at) VALUES(:id,:plan,:payload,:at) ON CONFLICT(account_id,plan_id) DO UPDATE SET payload=excluded.payload,evaluated_at=excluded.evaluated_at"
                    ),
                    {
                        "id": account,
                        "plan": saved["id"],
                        "payload": json.dumps(state, allow_nan=False),
                        "at": now.isoformat(),
                    },
                )
        except Exception:  # noqa: BLE001 - private plan details never enter logs
            log.warning("Rivalutazione sessione personale rinviata")


def device_candidates(subscription_id, now):
    with get_engine().connect() as con:
        rows = (
            con.execute(
                text(
                    "SELECT a.payload FROM personal_alerts a JOIN personal_devices d ON d.account_id=a.account_id WHERE d.subscription_id=:id"
                ),
                {"id": subscription_id},
            )
            .scalars()
            .all()
        )
    result = []
    for raw in rows:
        state = json.loads(raw)
        candidate = state.get("pending")
        if (
            candidate
            and now < utc(state["start"])
            and pd.Timedelta(0)
            <= now - utc(candidate["created_at"])
            <= pd.Timedelta(hours=1)
            and not quiet_now(state["rules"], now, state["timezone"])
        ):
            result.append({k: v for k, v in candidate.items() if k != "created_at"})
    return result
