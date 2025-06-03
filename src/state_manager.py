# state_manager.py

import json
import os
from typing import TypedDict

STATE_FILE = "state.json"

class State(TypedDict, total=False):
    skip: int
    page_limit: int
    total: int | None

# define the shape of your initial state
INITIAL_STATE: State = {
    "skip": 0,
    "page_limit": 30,
    "total": None,
}

def load_state() -> State:
    """Load state from disk, or return a fresh initial state."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
            # Convert lists back to sets for specific keys
            state["unique_subjects"] = set(state.get("unique_subjects", []))
            state["unique_places"] = set(state.get("unique_places", []))
            state["unique_names"] = set(state.get("unique_names", []))
            return state
    return INITIAL_STATE.copy()

def save_state(state: State) -> None:
    """Persist the given state dict to disk."""
    # Convert sets to lists before saving state
    state["unique_subjects"] = list(state.get("unique_subjects", []))
    state["unique_places"] = list(state.get("unique_places", []))
    state["unique_names"] = list(state.get("unique_names", []))
    
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def reset_state() -> None:
    """Overwrite state.json with the initial default values."""
    save_state(INITIAL_STATE.copy())
