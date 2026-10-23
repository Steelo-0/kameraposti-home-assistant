"""1.18.0 (Turva onboarding; steelo 2026-10-23 "2"): the integration is called "Turva ja Kameraposti", because
Kameraposti (kameraposti.fi) customers use the same integration; its domain stays kameraposti.

Renaming the domain would break every installation (entries, entity ids, the kameraposti_detection event,
the custom_components/kameraposti folder HACS installs into), so only the shown names change.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from custom_components.kameraposti.const import DOMAIN

ROOT = Path(__file__).parent.parent
INTEGRATION = ROOT / "custom_components" / "kameraposti"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_the_integration_is_called_turva_ja_kameraposti_and_its_domain_stays() -> None:
    manifest = _json(INTEGRATION / "manifest.json")

    assert manifest["name"] == "Turva ja Kameraposti"
    assert manifest["domain"] == DOMAIN == "kameraposti"
    assert manifest["documentation"] == "https://github.com/Steelo-0/kameraposti-home-assistant"
    assert _json(ROOT / "hacs.json")["name"] == "Turva ja Kameraposti"


def test_the_titles_fit_both_services() -> None:
    """The first form is shown before the service is known; reconnect and the sensor choice belong to one
    entry, which may be Turva's or Kameraposti's, so their titles name neither."""
    expected = {
        "fi": {
            "user": "Yhdistä Turvaan tai Kamerapostiin",
            "reauth_confirm": "Yhdistä uudelleen",
            "sensors": "Valitse anturit",
        },
        "en": {
            "user": "Connect to Turva or Kameraposti",
            "reauth_confirm": "Reconnect",
            "sensors": "Choose sensors",
        },
    }
    for language, titles in expected.items():
        translations = _json(INTEGRATION / "translations" / f"{language}.json")
        steps = {**translations["config"]["step"], **translations["options"]["step"]}
        for step, title in titles.items():
            assert steps[step]["title"] == title, (language, step)
        titled = {step: texts.get("title", "") for step, texts in steps.items()}
        named = [step for step, title in titled.items() if re.search("Turva|Kamerapost", title)]
        assert named == ["user"], language


def test_the_readme_uses_the_integration_name() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert readme.startswith("# Turva ja Kameraposti for Home Assistant\n")
    assert "search for\n   **Turva ja Kameraposti**" in readme


def test_the_readme_has_no_bare_placeholders() -> None:
    """Audit H-L4: GitHub and HACS read "<home name>" outside code as an HTML tag and show nothing."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    bare = []
    in_code_block = False
    for number, line in enumerate(readme.splitlines(), 1):
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            continue
        if not in_code_block and re.search(r"<[A-Za-z/][^>]*>", re.sub(r"`[^`]*`", "", line)):
            bare.append(number)
    assert bare == []
