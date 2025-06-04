import os
from typing import Dict, Any
from asnake.client import ASnakeClient

REPO_ID = os.getenv("REPOSITORY_ID", "2")

client = ASnakeClient(
    baseurl=os.environ["ARCHIVESSPACE_URL"],
    username=os.environ["ARCHIVESSPACE_USER"],
    password=os.environ["ARCHIVESSPACE_PASS"],
)
client.authorize()

def load_existing_resources() -> Dict[str, Dict[str, Any]]:
    ids = client.get(f"/repositories/{REPO_ID}/resources", params={"all_ids": True}).json()
    found: Dict[str, Dict[str, Any]] = {}
    for rid in ids:
        rec = client.get(f"/repositories/{REPO_ID}/resources/{rid}").json()
        if (id0 := rec.get("id_0")):
            found[id0] = {
                "rid": rid,
                "uri": rec["uri"],
                "lock_ver": rec["lock_version"],
            }
    return found

def load_existing_subjects() -> Dict[str, Dict[str, Any]]:
    ids = client.get("/subjects", params={"all_ids": True}).json()
    found: Dict[str, Dict[str, Any]] = {}
    for sid in ids:
        rec = client.get(f"/subjects/{sid}").json()
        if (id0 := rec.get("terms", [{}])[0].get("term")):
            found[id0] = {
                "sid": sid,
                "uri": rec["uri"],
                "lock_ver": rec["lock_version"],
            }
    return found

def load_existing_agents() -> Dict[str, Dict[str, Any]]:
    ids = client.get("/agents/corporate_entities", params={"all_ids": True}).json()
    found: Dict[str, Dict[str, Any]] = {}
    for aid in ids:
        rec = client.get(f"/agents/corporate_entities/{aid}").json()
        if (id0 := rec.get("names", [{}])[0].get("primary_name")):
            found[id0] = {
                "aid": aid,
                "uri": rec["uri"],
                "lock_ver": rec["lock_version"],
            }
    return found
