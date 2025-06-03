import logging, time, os, requests
import ssl
from urllib.error import URLError

from atom_helpers import fetch_atom_detail, fetch_slugs
from cache        import load_existing_resources, load_existing_subjects, load_existing_agents
from mapping      import build_resource_json
from updater      import upsert_resource, update_resource, delete_resource, create_subject, update_subject, create_corporate_agent, update_corporate_agent
from state_manager import load_state, save_state, reset_state

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

API_URL     = os.getenv("ATOM_API_URL", "https://search-bcarchives.royalbcmuseum.bc.ca/api").rstrip("/")
QUERY       = os.getenv("ATOM_INFORMATION_OBJECTS_QUERY", "sq0=GR*&sf0=referenceCode&levels=197")
PAGE_LIMIT  = 30
WAIT_SECONDS= int(os.getenv("ATOM_WAIT_SECONDS", "90"))

def process_batch(skip: int, cache: dict, processed_ids: set, state: dict) -> (int, int):
    slugs, total = fetch_slugs(skip, PAGE_LIMIT)
    
    if skip == 0:
        logging.info("Total information objects to process: %s", total)
        
    for i, rec in enumerate(slugs, start=1):
        slug = rec.get("slug") or rec.get("url_identifier") or rec.get("id")
        try:
            logging.info("Processing record %s of %s: %s", skip + i, total, slug)
            detail = fetch_atom_detail(slug)
            if not detail:
                continue  # Skip processing if detail is empty

            rsrc = build_resource_json(detail, slug)
            upsert_resource(rsrc, cache)
            processed_ids.add(rsrc["id_0"])

            # Extract access points and save them to state
            id_0 = rsrc["id_0"]
            state.setdefault("access_points", {})[id_0] = {
                "subject": detail.get("subject_access_points", []),
                "place": detail.get("place_access_points", []),
                "name": detail.get("name_access_points", []),
                "creator": detail.get("creators", []),
            }

            # Ensure state keys are initialized as sets
            state["unique_subjects"] = set(state.get("unique_subjects", []))
            state["unique_places"] = set(state.get("unique_places", []))
            state["unique_names"] = set(state.get("unique_names", []))

            # Update unique sets
            state["unique_subjects"].update(detail.get("subject_access_points", []))
            state["unique_places"].update(detail.get("place_access_points", []))
            state["unique_names"].update(detail.get("name_access_points", []))

            # Process creators as agents
            for creator in detail.get("creators", []):
                creator_data = {
                    "id_0": creator.get("authotized_form_of_name"),
                    "dates_of_existence": [creator.get("dates_of_existence")],
                }
                if creator_data["id_0"]:
                    state.setdefault("unique_names", set()).add(creator_data["id_0"])

        except URLError as e:
            logging.error("SSL or URL error while processing slug '%s': %s", slug, e)
            continue  # Move on to the next record
        except ssl.SSLError as e:
            logging.error("SSL error while processing slug '%s': %s", slug, e)
            continue  # Move on to the next record
        except Exception as e:
            logging.error("Error processing slug '%s': %s", slug, e)
            continue  # Move on to the next record

        if i < len(slugs):
            time.sleep(WAIT_SECONDS)

    return len(slugs), total or 0

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
            creator_id = creator.get("authotized_form_of_name")  # Corrected key
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

    # Ensure 'total' is initialized in the state
    if state.get("total") is None:
        state["total"] = float('inf')

    while state["skip"] < state.get("total"):  # Continue until all batches are processed
        try:
            if state["skip"] == 0:  # Set total on first batch
                processed, total = process_batch(state["skip"], cache, processed_ids, state)
                state["total"] = total
            else:
                logging.info("Batch %s → skipping %s", PAGE_LIMIT, state["skip"])
                processed, _ = process_batch(state["skip"], cache, processed_ids, state)

            state["skip"] += processed
            save_state(state)

            logging.info("Processed %s records.", state["skip"])
        except Exception as e:
            logging.error("An error occurred in the main loop: %s", e)
            continue

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
