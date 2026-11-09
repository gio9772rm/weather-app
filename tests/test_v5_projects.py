from copy import deepcopy

from v5_cielo import ObservingSite
from v5_projects import project_outlook, project_spec


def test_forecast_uses_remaining_filter_hours_and_never_credits_acquisition():
    site = ObservingSite("roma", "Roma", "Europe/Rome", 41.9, 12.5, 35)
    project = project_spec(
        {
            "target": "M81",
            "filters": [
                {
                    "name": "L",
                    "desired_seconds": 3600,
                    "acquired_seconds": 3600,
                    "exposure_seconds": 60,
                },
                {
                    "name": "R",
                    "desired_seconds": 7200,
                    "acquired_seconds": 3600,
                    "exposure_seconds": 120,
                },
            ],
        }
    )
    before = deepcopy(project)
    nights = {
        "2026-11-10": [{"start": "2026-11-10T20:01Z", "end": "2026-11-10T20:55Z"}],
        "2026-11-11": [{"start": "2026-11-11T20:00Z", "end": "2026-11-11T23:00Z"}],
        "2026-11-12": [],
    }
    options = {"setup_minutes": 15, "minimum_minutes": 30}
    outlook = project_outlook(site, project, nights, options)
    assert outlook["best"]["date"] == "2026-11-11"
    assert outlook["best"]["useful_seconds"] == outlook["missing_seconds"] == 3600
    assert [f["name"] for f in outlook["budget"]["filters"]] == ["R"]
    assert outlook["nights"][0]["useful_seconds"] < 3600
    assert outlook["nights"][2]["window"] is None
    assert project == before

    project["filters"][1]["acquired_seconds"] = 7200
    complete = project_outlook(site, project, nights, options)
    assert complete["best"] is None and complete["missing_seconds"] == 0
