"""Latest public V5 panels for Pro, using the already published station snapshot."""

from copy import deepcopy
from math import isfinite

import pandas as pd
import streamlit as st

from v56_probability_calibration import METHOD, public_windows


def stamp(value, timezone):
    at = pd.to_datetime(value, utc=True, errors="coerce")
    return "—" if pd.isna(at) else at.tz_convert(timezone).strftime("%d/%m/%Y %H:%M")


def number(value, digits=0):
    try:
        return f"{float(value):.{digits}f}" if isfinite(float(value)) else "—"
    except (ValueError, TypeError):
        return "—"


def recent(value, now, hours=26):
    at = pd.to_datetime(value, utc=True, errors="coerce")
    return pd.notna(at) and at <= now <= at + pd.Timedelta(hours=hours)


def calibration_active(snapshot, now):
    c = snapshot.get("window_calibration") or {}
    p = snapshot.get("window_probabilities") or {}
    expires = pd.to_datetime(c.get("expires_at"), utc=True, errors="coerce")
    return bool(
        c.get("status") == "active"
        and c.get("method") == METHOD
        and c.get("station_id") == p.get("station_id")
        and c.get("model") == p.get("model")
        and recent(c.get("evaluated_at"), now, 12)
        and pd.notna(expires)
        and now < expires
    )


def upcoming_windows(snapshot, now):
    product = deepcopy(snapshot.get("window_probabilities") or {})
    if not recent(product.get("acquired_at"), now, 12):
        return []
    product = public_windows(product, snapshot.get("window_calibration"), now)
    return [
        row
        for row in product.get("rows", [])
        if pd.to_datetime(row.get("start"), utc=True, errors="coerce") >= now
        and number(row.get("probability")) != "—"
    ]


def _publication(snapshot, timezone):
    st.caption(
        f"Fotografia pubblicata: {stamp(snapshot.get('generated_at'), timezone)} · {timezone}"
    )
    if not recent(snapshot.get("generated_at"), pd.Timestamp.now(tz="UTC"), 1):
        st.warning(
            "Fotografia da aggiornare: controlla l’orario prima di usare questi dati."
        )


def render_windows(snapshot, timezone):
    st.subheader("Quanto può durare la finestra?")
    st.write(
        "Gli stessi scenari ensemble della vista normale, per la località selezionata."
    )
    if not snapshot:
        st.info("Finestre in preparazione nel prossimo ciclo meteo.")
        return
    _publication(snapshot, timezone)
    product = snapshot.get("window_probabilities") or {}
    rows = upcoming_windows(snapshot, pd.Timestamp.now(tz="UTC"))
    for column, (kind, title) in zip(
        st.columns(2),
        (("dry", "Due ore asciutte"), ("photo", "Tre ore per fotografare")),
    ):
        with column:
            st.markdown(f"#### {title}")
            best = sorted(
                (r for r in rows if r.get("kind") == kind),
                key=lambda r: (-float(r["probability"]), r["start"]),
            )[:3]
            if not best:
                st.info("Nessuna finestra con dati completi e recenti.")
            for row in best:
                with st.container(border=True):
                    st.metric(
                        "Probabilità della finestra", number(row["probability"]) + "%"
                    )
                    st.write(
                        f"{stamp(row['start'], timezone)} → {stamp(row.get('end'), timezone)}"
                    )
                    origin = (
                        "Calibrata"
                        if row.get("calibration_id")
                        else "Frequenza ensemble originale"
                    )
                    st.caption(
                        f"{origin} · {number(row.get('successful_members'))}/{number(row.get('members'))} membri completi"
                    )
                    if row.get("calibration_id"):
                        st.caption(
                            f"Frequenza originale: {number(row.get('raw_probability'))}%"
                        )
    st.caption(
        f"Fonte: {product.get('model') or 'In attesa'} · acquisita {stamp(product.get('acquired_at'), timezone)}"
    )
    st.write(
        product.get("note") or "In attesa della prima acquisizione dei singoli membri."
    )
    st.caption(
        "Le frequenze ensemble descrivono scenari. Non sono una percentuale generale di accuratezza."
    )
    verification = snapshot.get("window_validation") or {}
    with st.expander("Verifica sulle finestre realmente osservate"):
        st.write(
            f"{number(verification.get('n'))} finestre verificate · Brier {number(verification.get('brier'), 3)} (0 = errore minimo)."
        )
        bins = verification.get("bins") or []
        if bins:
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "Fascia prevista": f"{number(b.get('low'))}–{number(b.get('high'))}%",
                            "Finestre": b.get("n"),
                            "Media prevista %": b.get("forecast"),
                            "Frequenza osservata %": b.get("observed"),
                        }
                        for b in bins
                    ]
                ),
                hide_index=True,
                width="stretch",
            )


def render_precision(snapshot, timezone):
    st.subheader("Quanto ci prende?")
    if not snapshot:
        st.info("Riscontri in preparazione nel prossimo ciclo meteo.")
        return
    _publication(snapshot, timezone)
    p = snapshot.get("window_validation") or {}
    c = snapshot.get("window_calibration") or {}
    r = c.get("readiness") or p.get("readiness") or {}
    collection = p.get("collection") or {}
    st.markdown("### Campioni per la calibrazione")
    st.write(
        "Finestre asciutte di due ore previste 6–12 ore prima, verificate sulla stessa stazione."
    )
    columns = st.columns(3)
    columns[0].metric(
        "Finestre verificate",
        number(p.get("n")),
        f"{number(r.get('days'))} giorni con riscontri",
        delta_color="off",
    )
    columns[1].metric(
        "Asciutte / con pioggia", f"{number(r.get('dry'))} / {number(r.get('wet'))}"
    )
    columns[2].metric("Cicli archiviati", number(collection.get("cycles")))
    st.caption(
        f"{number(collection.get('pending'))} finestre da attendere · {number(collection.get('missing_observations'))} con osservazioni incomplete · copertura {number(collection.get('coverage_percent'))}%"
    )
    requirements = r.get("requirements") or []
    if requirements:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "Requisito": g.get("label"),
                        "Raccolti": g.get("current"),
                        "Minimo": g.get("minimum"),
                        "Mancano": 0 if g.get("met") else g.get("remaining"),
                    }
                    for g in requirements
                ]
            ),
            hide_index=True,
            width="stretch",
        )
    else:
        st.info("Riepilogo dei requisiti in preparazione nel normale ciclo meteo.")
    st.write(
        r.get("note")
        or "La raccolta dei campioni non implica l’attivazione della calibrazione."
    )
    st.caption(f"Verifica campioni: {stamp(p.get('evaluated_at'), timezone)}")
    if p and not recent(p.get("evaluated_at"), pd.Timestamp.now(tz="UTC")):
        st.warning("Statistiche datate: controlla l’orario della verifica.")
    active = calibration_active(snapshot, pd.Timestamp.now(tz="UTC"))
    if active:
        st.success(
            "Calibrazione attiva per le finestre asciutte di due ore previste 6–12 ore prima."
        )
    else:
        st.info(
            "Calibrazione delle probabilità non attiva: restano disponibili le frequenze originali."
        )
    st.write(c.get("reason") or "In attesa della valutazione automatica.")
    validation = c.get("validation") or {}
    if "raw_brier" in validation:
        holdout = validation.get("holdout") or {}
        st.write(
            f"Prova separata su {number(holdout.get('n'))} finestre in {number(holdout.get('days'))} giorni. Brier originale {number(validation.get('raw_brier'), 3)}, corretto {number(validation.get('calibrated_brier'), 3)}, riferimento {number(validation.get('reference_brier'), 3)}. Miglioramento {number(validation.get('improvement_percent'))}%."
        )
    st.caption(
        f"Controllo: {stamp(c.get('evaluated_at'), timezone)} · scadenza: {stamp(c.get('expires_at'), timezone)}"
    )
    _render_comparison(snapshot.get("insights", {}).get("comparison") or {}, timezone)
    _render_alerts(snapshot.get("alert_verification") or {}, timezone)


def _render_comparison(product, timezone):
    st.markdown("### Confronto alla pari")
    columns = st.columns(4)
    days = columns[0].selectbox(
        "Periodo · giorni", [7, 30, 90], index=1, key="paired_days"
    )
    lead = columns[1].selectbox(
        "Informazioni disponibili · ore prima", [6, 24, 72], index=1, key="paired_lead"
    )
    labels = {
        "temp_c": "Temperatura · °C",
        "humidity": "Umidità · %",
        "wind_kmh": "Vento · km/h",
        "rain_mm": "Pioggia · mm/h",
    }
    variable = columns[2].selectbox(
        "Parametro", list(labels), format_func=labels.get, key="paired_variable"
    )
    basis = columns[3].selectbox(
        "Provenienza",
        ["live", "previous_run"],
        format_func=lambda v: (
            "Acquisizioni operative" if v == "live" else "Archivio retrospettivo"
        ),
        key="paired_basis",
    )
    rows = [
        r
        for r in product.get("rows", [])
        if r.get("window_days") == days
        and r.get("lead_hours") == lead
        and r.get("variable") == variable
        and r.get("basis") == basis
    ]
    table = []
    for row in rows:
        ci = row.get("delta_ci95") or []
        evidence = row.get("evidence", "uncertain")
        table.append(
            {
                "Modello A": row.get("left"),
                "Modello B": row.get("right"),
                "MAE A": row.get("mae_left"),
                "MAE B": row.get("mae_right"),
                "Ore comuni": row.get("n"),
                "Giorni comuni": row.get("days"),
                "Periodo effettivo": f"{stamp(row.get('start'), timezone)} → {stamp(row.get('end'), timezone)}",
                "Differenza A−B · IC 95%": f"{number(row.get('delta_mae'), 2)} [{number(ci[0], 2)}; {number(ci[1], 2)}]"
                if row.get("adequate") and len(ci) == 2
                else "Campione insufficiente",
                "Evidenza": "Differenza non dimostrata"
                if evidence == "uncertain"
                else f"Errore inferiore: {row.get(evidence, '—')}",
            }
        )
    if table:
        st.dataframe(pd.DataFrame(table), hide_index=True, width="stretch")
    else:
        st.info(
            "Servono almeno dodici ore comuni. La mancanza di campioni non indica equivalenza dei modelli."
        )
    st.caption(
        product.get("method")
        or "Confronti sulle stesse osservazioni, in preparazione nel normale ciclo di pubblicazione."
    )
    st.caption(
        f"Calcolato: {stamp(product.get('evaluated_at'), timezone)}. Confronta A e B nella stessa riga: le coppie possono avere coperture diverse."
    )


def _render_alerts(product, timezone):
    st.markdown("### Quanto funzionano le soglie degli avvisi?")
    st.write(
        "Confronto sugli stessi intervalli, anche senza segnale. I numeri contano ore, non temporali o notifiche ricevute."
    )
    rows = product.get("rows") or []
    if not rows:
        st.info("Verifica delle soglie in preparazione nel prossimo ciclo meteo.")
        return
    if not recent(product.get("evaluated_at"), pd.Timestamp.now(tz="UTC")):
        st.warning(
            "Verifica delle soglie datata: controlla l’orario della valutazione."
        )
    for kind, title in (
        ("rain", "Pioggia · probabilità e quantità prevista positiva"),
        ("wind", "Vento · raffica"),
    ):
        st.markdown(f"#### {title}")
        selected = [
            {
                "Soglia": f"{number(r.get('threshold'))} {'%' if kind == 'rain' else 'km/h'}",
                "Ore": r.get("n"),
                "Giorni": r.get("days"),
                "Centrate": r.get("hits"),
                "Falsi allarmi": r.get("false_alarms"),
                "Mancate": r.get("misses"),
                "Assenze corrette": r.get("correct_negatives"),
                "Segnali confermati %": r.get("precision_percent"),
                "Eventi rilevati %": r.get("recall_percent"),
                "Previsioni mancanti": r.get("missing_forecast"),
                "Osservazioni incomplete": r.get("missing_observations"),
            }
            for r in rows
            if r.get("kind") == kind
        ]
        if selected:
            st.dataframe(pd.DataFrame(selected), hide_index=True, width="stretch")
    st.caption(
        product.get("note")
        or "Segnali confermati = centrate / (centrate + falsi allarmi); eventi rilevati = centrate / (centrate + mancate)."
    )
    st.caption(
        f"Valutazione: {stamp(product.get('evaluated_at'), timezone)} · periodo {number(product.get('period_days'))} giorni."
    )
