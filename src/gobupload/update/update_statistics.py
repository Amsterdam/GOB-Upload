from gobcore.logging.logger import logger


class UpdateStatistics():

    def __init__(self, events):
        self.stored = {}
        self.skipped = {}
        self.applied = {}
        self.num_events = 0
        self.num_single_events = 0
        self.num_bulk_events = 0

    def _count(self, event):
        action = event["event"]
        self.num_events += 1
        if action == "BULKCONFIRM":
            self.num_bulk_events += 1
            return len(event['data']['confirms'])
        else:
            self.num_single_events += 1
            return 1

    def _action(selfself, event):
        action = event["event"]
        if action == "BULKCONFIRM":
            action = "CONFIRM"
        return action

    def count_event(self, event):
        self._add_stored(self._action(event), self._count(event))

    def skip_event(self, event):
        self._add_skipped(self._action(event), self._count(event))

    def _add_stored(self, action, n=1):
        self.stored[action] = self.stored.get(action, 0) + n

    def _add_skipped(self, action, n=1):
        self.skipped[action] = self.skipped.get(action, 0) + n

    def add_applied(self, action, n=1):
        self.applied[action] = self.applied.get(action, 0) + n

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
                            (self.applied, "{action} events  applied")]:
            for action, n in result.items():
                results[fmt.format(action=action)] = n

        return results

    def log(self):
        for process in ["stored", "skipped", "applied"]:
            for action, n in getattr(self, process).items():
                logger.info(f"{n} {action} events {process}")
