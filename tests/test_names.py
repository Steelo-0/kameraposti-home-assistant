"""1.18.0 (Turva onboarding, steelo "1 ok"): the integration is called Turva; its domain stays kameraposti.

Renaming the domain would break every installation (entries, entity ids, the kameraposti_detection event,
the custom_components/kameraposti folder HACS installs into), so only the shown names change.
"""

from __future__ import annotations

import json
from pathlib import Path

from custom_components.kameraposti.const import DOMAIN

ROOT = Path(__file__).parent.parent
INTEGRATION = ROOT / "custom_components" / "kameraposti"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_the_integration_is_called_turva_and_its_domain_stays() -> None:
    manifest = _json(INTEGRATION / "manifest.json")

    assert manifest["name"] == "Turva"
    assert manifest["domain"] == DOMAIN == "kameraposti"
    assert manifest["documentation"] == "https://github.com/Steelo-0/kameraposti-home-assistant"
    assert _json(ROOT / "hacs.json")["name"] == "Turva"


def test_the_titles_that_named_kameraposti_name_turva() -> None:
    expected = {
        "fi": {
            "user": "Yhdistä Turvaan",
            "reauth_confirm": "Yhdistä uudelleen Turvaan",
            "sensors": "Anturit Turvaan",
        },
        "en": {
            "user": "Connect to Turva",
            "reauth_confirm": "Reconnect to Turva",
            "sensors": "Sensors to Turva",
        },
    }
    for language, titles in expected.items():
        translations = _json(INTEGRATION / "translations" / f"{language}.json")
        steps = {**translations["config"]["step"], **translations["options"]["step"]}
        for step, title in titles.items():
            assert steps[step]["title"] == title, (language, step)
        assert not [step for step, texts in steps.items() if "Kamerapost" in texts.get("title", "")], language


def test_the_readme_is_turvas() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert readme.startswith("# Turva for Home Assistant\n")
    assert "search for\n   **Turva**" in readme
