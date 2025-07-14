import re
from typing import Any, Dict, List, Optional
from datetime import datetime

ALLOWED_SINGLEPART = {"abstract", "materialspec", "physdesc", "physfacet", "physloc"}

def build_extents(extent_str: str) -> List[Dict[str, str]]:
    """Convert free‑text extent strings to a custom extent type 'Entry'."""
    if not extent_str:
        return []

    return [
        {
            "number": "1",
            "extent_type": "Entry",
            "portion": "whole",
            "physical_details": extent_str.strip()
        }
    ]


def make_note(note_type: str, content: str | None) -> Optional[Dict[str, Any]]:
    """Return a valid ArchivesSpace note block, or ``None`` if no content."""
    if not content:
        return None

    if note_type in ALLOWED_SINGLEPART:
        return {
            "jsonmodel_type": "note_singlepart",
            "type": note_type,
            "publish": True,
            "content": [content],
        }

    return {
        "jsonmodel_type": "note_multipart",
        "type": note_type,
        "publish": True,
        "subnotes": [
            {
                "jsonmodel_type": "note_text",
                "content": content,
                "publish": True,
            }
        ],
    }

def build_resource_json(d: Dict[str, Any], id: str) -> Dict[str, Any]:
    """Transform a CSV record into an ArchivesSpace resource JSON."""
    notes = [
        make_note("scopecontent", d.get("scopeAndContent")),
        make_note("accessrestrict", d.get("accessConditions")),
        make_note(
            "originalsloc",
            f"<extref target='_blank' href='https://search-bcarchives.royalbcmuseum.bc.ca/informationobject/browse?sq0={d.get('referenceCode', "")}'>"
            f"{d.get('referenceCode', "")} - {d.get('title', 'Untitled')}</extref><br />"
            f"<emph>Source: BC Museum Archives</emph><br />"
            f"<emph>Indexed: </emph><date>{datetime.now().strftime('%Y-%m-%d')}</date>"
        ),
    ]
    notes = [n for n in notes if n]

    extents = build_extents(d.get("extentAndMedium", "")) or [
        {"number": "0", "extent_type": "volumes", "portion": "whole"}
    ]

    return {
        "title": d.get("title", "Untitled"),
        "id_0": d.get("referenceCode", "Unknown"),
        "level": d.get("levelOfDescription", "series").lower(),
        "publish": d.get("publicationStatus", "").lower() == "published",
        "repository_processing_note": f"Data acquired via automated script on {datetime.now().strftime('%Y-%m-%d-%H-%M')}. Please visit the BC Museum Archives access catalogue for the current and authoritative description.",
        "dates": [
            {
                "label": "creation",
                "date_type": "inclusive",
                "expression": d.get("eventDates") or d.get("eventStartDates") or "n.d.",
            }
        ],
        "extents": extents,
        "lang_materials": [
            {"language_and_script": {"language": "und", "script": "Latn"}}
        ],
        "finding_aid_language": "eng",
        "finding_aid_script": "Zyyy",
        "notes": notes,
    }
