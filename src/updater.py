import json
import logging
from typing import Any, Dict
from cache import client, REPO_ID

def fetch_existing_data(uri: str) -> Dict[str, Any]:
    """Fetch the existing data for a given URI."""
    resp = client.get(uri)
    if resp.ok:
        return resp.json()
    else:
        logging.error("Failed to fetch existing data for URI %s: %s", uri, resp.text)
        return {}

def update_resource(rsrc: Dict[str, Any], meta: Dict[str, Any]) -> None:
    existing_data = fetch_existing_data(meta["uri"])
    if not existing_data:
        logging.error("Cannot update resource %s: Failed to fetch existing data", rsrc["id_0"])
        return

    # Fetch the latest lock_version
    latest_lock_version = existing_data.get("lock_version")
    if latest_lock_version is None:
        logging.error("Cannot update resource %s: Missing lock_version", rsrc["id_0"])
        return

    # Merge existing data with the new data
    updated_data = {**existing_data, **rsrc}
    updated_data["uri"] = meta["uri"]
    updated_data["lock_version"] = latest_lock_version

    resp = client.post(meta["uri"], json=updated_data)
    if resp.ok:
        logging.info("✔ Updated %s", rsrc["id_0"])
    elif resp.status_code == 409 and "modified since you fetched it" in resp.text:
        logging.warning("Conflict detected for %s. Refetching lock_version and retrying.", rsrc["id_0"])
        # Refetch the latest lock_version and retry
        existing_data = fetch_existing_data(meta["uri"])
        latest_lock_version = existing_data.get("lock_version")
        if latest_lock_version is None:
            logging.error("Cannot update resource %s: Missing lock_version after refetch", rsrc["id_0"])
            return

        updated_data["lock_version"] = latest_lock_version
        resp = client.post(meta["uri"], json=updated_data)
        if resp.ok:
            logging.info("✔ Updated %s after retry", rsrc["id_0"])
        else:
            logging.error("✖ Update failed for %s after retry: %s", rsrc["id_0"], resp.text)
    else:
        logging.error("✖ Update failed for %s: %s", rsrc["id_0"], resp.text)

def upsert_resource(rsrc: Dict[str, Any], cache: Dict[str, Dict[str, Any]]) -> None:
    ident = rsrc["id_0"]
    if ident in cache:
        return update_resource(rsrc, cache[ident])
    resp = client.post(f"/repositories/{REPO_ID}/resources", json=rsrc)
    if resp.ok:
        body = resp.json()
        cache[ident] = {
            "rid": body["id"], "uri": body["uri"], "lock_ver": body["lock_version"]
        }
        logging.info("✔ Created %s", ident)
    else:
        logging.error("✖ Create failed for %s: %s", ident, resp.text)

def delete_resource(meta: Dict[str, Any]) -> None:
    resp = client.delete(meta["uri"])
    if resp.ok:
        logging.info("✔ Deleted resource with id_0: %s", meta["id_0"])
    else:
        logging.error("✖ Failed to delete resource with id_0: %s", meta["id_0"])

def create_subject(subject: Dict[str, Any], cache: Dict[str, Dict[str, Any]]) -> None:
    payload = {
        "jsonmodel_type": "subject",
        "external_ids": [],
        "publish": True,
        "is_slug_auto": True,
        "used_within_repositories": [],
        "used_within_published_repositories": [],
        "terms": [
            {
                "jsonmodel_type": "term",
                "term": subject["id_0"],
                "term_type": subject.get("term_type", "topical"),
                "vocabulary": "/vocabularies/1",
            }
        ],
        "external_documents": [],
        "vocabulary": "/vocabularies/1",
        "source": subject.get("source", "lcsh")
    }

    resp = client.post(f"/subjects", json=payload)
    if resp.ok:
        body = resp.json()
        cache[subject["id_0"]] = {
            "sid": body["id"], "uri": body["uri"], "lock_ver": body["lock_version"]
        }
        logging.info("✔ Created subject %s", subject["id_0"])
    else:
        logging.error("✖ Create failed for subject %s: %s", subject["id_0"], resp.text)

def update_subject(subject: Dict[str, Any], meta: Dict[str, Any]) -> None:
    existing_data = fetch_existing_data(meta["uri"])
    if not existing_data:
        logging.error("Cannot update subject %s: Failed to fetch existing data", subject["id_0"])
        return

    # Merge existing data with the new data
    updated_data = {**existing_data, **subject}
    updated_data["uri"] = meta["uri"]
    updated_data["lock_version"] = meta["lock_ver"]

    resp = client.post(meta["uri"], json=updated_data)
    if resp.ok:
        logging.info("✔ Updated subject %s", subject["id_0"])
    elif resp.status_code == 409 and "modified since you fetched it" in resp.text:
        logging.warning("Conflict detected for subject %s. Refetching lock_version and retrying.", subject["id_0"])
        # Refetch the latest lock_version and retry
        existing_data = fetch_existing_data(meta["uri"])
        latest_lock_version = existing_data.get("lock_version")
        if latest_lock_version is None:
            logging.error("Cannot update subject %s: Missing lock_version after refetch", subject["id_0"])
            return

        updated_data["lock_version"] = latest_lock_version
        resp = client.post(meta["uri"], json=updated_data)
        if resp.ok:
            logging.info("✔ Updated subject %s after retry", subject["id_0"])
        else:
            logging.error("✖ Update failed for subject %s after retry: %s", subject["id_0"], resp.text)
    else:
        logging.error("✖ Update failed for subject %s: %s", subject["id_0"], resp.text)

def delete_subject(meta: Dict[str, Any]) -> None:
    resp = client.delete(meta["uri"])
    if resp.ok:
        logging.info("✔ Deleted subject with id_0: %s", meta["id_0"])
    else:
        logging.error("✖ Failed to delete subject with id_0: %s", meta["id_0"])

def create_corporate_agent(agent: Dict[str, Any], cache: Dict[str, Dict[str, Any]]) -> None:
    # Ensure the 'names' field has at least one item
    names = agent.get("names", [])
    if not names:
        names = [{
            "jsonmodel_type": "name_corporate_entity",
            "primary_name": agent["id_0"],
            "sort_name": agent["id_0"],
            "authority_id": "",
            "rules": "",
            "source": "local"
        }]

    payload = {
        "jsonmodel_type": "agent_corporate_entity",
        "agent_contacts": agent.get("agent_contacts", []),
        "dates_of_existence": agent.get("dates_of_existence", []),
        "is_slug_auto": True,
        "publish": True,
        "names": names,
        "agent_type": "agent_corporate_entity"
    }

    resp = client.post(f"/agents/corporate_entities", json=payload)
    if resp.ok:
        body = resp.json()
        cache[agent["id_0"]] = {
            "aid": body["id"], "uri": body["uri"], "lock_ver": body["lock_version"]
        }
        logging.info("✔ Created corporate agent %s", agent["id_0"])
    else:
        logging.error("✖ Create failed for corporate agent %s: %s", agent["id_0"], resp.text)

def update_corporate_agent(agent: Dict[str, Any], meta: Dict[str, Any]) -> None:
    existing_data = fetch_existing_data(meta["uri"])
    if not existing_data:
        logging.error("Cannot update corporate agent %s: Failed to fetch existing data", agent["id_0"])
        return

    # Merge existing data with the new data
    updated_data = {**existing_data, **agent}
    updated_data["uri"] = meta["uri"]
    updated_data["lock_version"] = meta["lock_ver"]

    resp = client.post(meta["uri"], json=updated_data)
    if resp.ok:
        logging.info("✔ Updated corporate agent %s", agent["id_0"])
    elif resp.status_code == 409 and "modified since you fetched it" in resp.text:
        logging.warning("Conflict detected for corporate agent %s. Refetching lock_version and retrying.", agent["id_0"])
        # Refetch the latest lock_version and retry
        existing_data = fetch_existing_data(meta["uri"])
        latest_lock_version = existing_data.get("lock_version")
        if latest_lock_version is None:
            logging.error("Cannot update corporate agent %s: Missing lock_version after refetch", agent["id_0"])
            return

        updated_data["lock_version"] = latest_lock_version
        resp = client.post(meta["uri"], json=updated_data)
        if resp.ok:
            logging.info("✔ Updated corporate agent %s after retry", agent["id_0"])
        else:
            logging.error("✖ Update failed for corporate agent %s after retry: %s", agent["id_0"], resp.text)
    else:
        logging.error("✖ Update failed for corporate agent %s: %s", agent["id_0"], resp.text)

def delete_corporate_agent(meta: Dict[str, Any]) -> None:
    resp = client.delete(meta["uri"])
    if resp.ok:
        logging.info("✔ Deleted corporate agent with id_0: %s", meta["id_0"])
    else:
        logging.error("✖ Failed to delete corporate agent with id_0: %s", meta["id_0"])
