"""
Update statistics

Gathers statistices about the update process
"""
from gobcore.logging.logger import logger


class UpdateStatistics():

    def __init__(self):
        self.stored = {}
        self.skipped = {}
        self.applied = {}
        self.num_events = 0
        self.num_single_events = 0
        self.num_bulk_events = 0

    def _update_counts(self, event):
        action = event["event"]
        self.num_events += 1
        if action == "BULKCONFIRM":
            self.num_bulk_events += 1
        else:
            self.num_single_events += 1

    def _count(self, event):
        action = event["event"]
        if action == "BULKCONFIRM":
            return len(event['data']['confirms'])
        else:
            return 1

    def _action(selfself, event):
        action = event["event"]
        if action == "BULKCONFIRM":
            action = "CONFIRM"
        return action

    def store_event(self, event):
        action = self._action(event)
        self.stored[action] = self.stored.get(action, 0) + self._count(event)
        self._update_counts(event)

    def skip_event(self, event):
        action = self._action(event)
        self.skipped[action] = self.skipped.get(action, 0) + self._count(event)
        self._update_counts(event)

    def add_applied(self, action, count):
        self.applied[action] = self.applied.get(action, 0) + count

    def results(self):
        """Get statistics in a dictionary

        :return:
        """
        results = {
            "Total events": self.num_events,
            "Single events": self.num_single_events,
            "Bulk events": self.num_bulk_events
        }
        for result, fmt in [(self.stored, "{action} events stored"),
                            (self.skipped, "{action} events skipped"),
                            (self.applied, "{action} events applied")]:
            for action, n in result.items():
                results[fmt.format(action=action)] = n

        return results

    def log(self):
        for process in ["stored", "skipped", "applied"]:
            for action, n in getattr(self, process).items():
                logger.info(f"{n} {action} events {process}")
