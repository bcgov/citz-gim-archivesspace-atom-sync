import logging, time, os, requests, csv

from cache        import load_existing_resources, load_existing_subjects, load_existing_agents
from csv_mapping  import build_resource_json
from updater      import upsert_resource, update_resource, delete_resource, create_subject, update_subject, create_corporate_agent, update_corporate_agent
from state_manager import load_state, save_state, reset_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

WAIT_SECONDS= int(os.getenv("ATOM_WAIT_SECONDS", "1"))

def read_csv_records(csv_path: str):
    """Read records from the CSV file and yield each as a dict."""
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield row

def process_all_records(cache: dict, processed_ids: set, state: dict) -> int:
    csv_path = os.path.join(os.path.dirname(__file__), 'data.csv')
    total = 0
    for i, detail in enumerate(read_csv_records(csv_path), start=1):
        identifier = detail.get("referenceCode") or detail.get("identifier") or str(i)
        try:
            logging.info("Processing record %s: %s", i, identifier)
            rsrc = build_resource_json(detail, identifier)
            upsert_resource(rsrc, cache)
            processed_ids.add(rsrc["id_0"])

            # Extract access points and save them to state
            id_0 = rsrc["id_0"]
            state.setdefault("access_points", {})[id_0] = {
                "subject": [item for item in detail.get("subjectAccessPoints", "").split("|") if item and item.upper() != "NULL"] if detail.get("subjectAccessPoints") else [],
                "place": [item for item in detail.get("placeAccessPoints", "").split("|") if item and item.upper() != "NULL"] if detail.get("placeAccessPoints") else [],
                "name": [item for item in detail.get("nameAccessPoints", "").split("|") if item and item.upper() != "NULL"] if detail.get("nameAccessPoints") else [],
                "creator": [item for item in detail.get("eventActors", "").split("|") if item and item.upper() != "NULL"] if detail.get("eventActors") else [],
            }

            # Ensure state keys are initialized as sets
            state["unique_subjects"] = set(state.get("unique_subjects", []))
            state["unique_places"] = set(state.get("unique_places", []))
            state["unique_names"] = set(state.get("unique_names", []))

            # Update unique sets
            state["unique_subjects"].update(state["access_points"][id_0]["subject"])
            state["unique_places"].update(state["access_points"][id_0]["place"])
            state["unique_names"].update(state["access_points"][id_0]["name"])

            # Process creators as agents (if needed, can be expanded)
            for creator in state["access_points"][id_0]["creator"]:
                if creator:
                    state.setdefault("unique_names", set()).add(creator)

        except Exception as e:
            logging.error("Error processing record '%s': %s", identifier, e)
            continue  # Move on to the next record

        time.sleep(WAIT_SECONDS)
        total += 1
    return total

def process_access_points(state, cache):
    # Process unique subjects
    for subject in state.get("unique_subjects", []):
        subject_data = {
            "source": "local",
            "term_type": "topical",
            "id_0": subject,
        }
        try:
            if subject in cache:
                logging.info("Updating existing subject: %s", subject)
                update_subject(subject_data, cache[subject])
            else:
                logging.info("Creating new subject: %s", subject)
                create_subject(subject_data, cache)
        except requests.exceptions.RequestException as e:
            logging.error("✖ Create/Update failed for subject %s: %s", subject, e.response.json())

    # Process unique places
    for place in state.get("unique_places", []):
        place_data = {
            "source": "local",
            "term_type": "geographic",
            "id_0": place,
        }
        try:
            if place in cache:
                logging.info("Updating existing place: %s", place)
                update_subject(place_data, cache[place])
            else:
                logging.info("Creating new place: %s", place)
                create_subject(place_data, cache)
        except requests.exceptions.RequestException as e:
            logging.error("✖ Create/Update failed for place %s: %s", place, e.response.json())

    # Process unique names
    for name in state.get("unique_names", []):
        agent_data = {
            "id_0": name,
        }
        try:
            if name in cache:
                logging.info("Updating existing corporate agent: %s", name)
                update_corporate_agent(agent_data, cache[name])
            else:
                logging.info("Creating new corporate agent: %s", name)
                create_corporate_agent(agent_data, cache)
        except requests.exceptions.RequestException as e:
            logging.error("✖ Create/Update failed for corporate agent %s: %s", name, e.response.json())

    # Update resources to link subjects and agents
    for resource_id, access_points in state.get("access_points", {}).items():
        resource_meta = cache.get(resource_id)
        if not resource_meta:
            logging.warning("Resource ID %s not found in cache. Skipping.", resource_id)
            continue

        linked_subjects = [
            {"ref": cache[sub]["uri"]} for sub in access_points.get("subject", []) if sub in cache
        ]
        linked_places = [
            {"ref": cache[place]["uri"]} for place in access_points.get("place", []) if place in cache
        ]
        linked_agents = [
            {"ref": cache[name]["uri"], "role": "subject"} for name in access_points.get("name", []) if name in cache
        ]

        # Add creators with role "subject" and only the first creator with role "creator"
        linked_creators = []
        for idx, creator in enumerate(access_points.get("creator", [])):
            creator_id = creator  # Treat creator as a string from CSV
            if creator_id in cache:
                if idx == 0:  # Add only the first creator with role "creator"
                    linked_creators.append({"ref": cache[creator_id]["uri"], "role": "creator"})
                else:
                    linked_creators.append({"ref": cache[creator_id]["uri"], "role": "subject"})

        # Update only the necessary fields while preserving existing properties
        resource_update = {
            "id_0": resource_id,
            "subjects": linked_subjects + linked_places,
            "linked_agents": linked_agents + linked_creators,
        }

        try:
            logging.info("Updating resource %s with linked subjects and agents.", resource_id)
            update_resource(resource_update, resource_meta)
        except requests.exceptions.RequestException as e:
            logging.error("✖ Update failed for resource %s: %s", resource_id, e.response.json())

def main():
    state = load_state()
    cache = load_existing_resources()

    # Load existing subjects and agents into the cache
    cache.update(load_existing_subjects())
    cache.update(load_existing_agents())

    processed_ids = set()

    # Process all records (no batching, no skip)
    total = process_all_records(cache, processed_ids, state)
    state["total"] = total
    save_state(state)
    logging.info("Processed %s records.", total)

    # Call process_access_points after processing resources
    process_access_points(state, cache)

    # Delete unused resources
    unused_ids = set(cache.keys()) - processed_ids
    for unused_id in unused_ids:
        time.sleep(WAIT_SECONDS)
        delete_resource(cache[unused_id])
        del cache[unused_id]

    # Reset state back to initial defaults
    reset_state()
    logging.info("state.json has been reset to initial values.")
    

if __name__ == "__main__":
    main()
