import asyncio
import uuid
from dataclasses import dataclass


@dataclass
class RunControl:
    interrupted: asyncio.Event


class ActiveRuns:
    _runs: dict[uuid.UUID, RunControl]

    def __init__(self):
        self._runs = {}

    def run_ids(self):
        return list(self._runs.keys())

    def register(self, run_id: uuid.UUID):
        control = RunControl(interrupted=asyncio.Event())
        self._runs[run_id] = control
        return control

    def unregister(self, run_id: uuid.UUID):
        self._runs.pop(run_id)

    def interrupt(self, run_id: uuid.UUID):
        control = self._runs.get(run_id, None)
        if control is not None:
            control.interrupted.set()
