"""
Compare statistics

Collects statistics about the compare process
"""


class CompareStatistics():

    def __init__(self):
        self.collected = 0
        self.compared = {}

    def collect(self, entity):
        """
        Adds 1 to the number of collected entities

        :param entity:
        :return:
        """
        self.collected += 1

    def compare(self, row):
        """
        Adds 1 to the counter for the specific row type (e.g. ADD, DELETE, ...)
        :param row:
        :return:
        """
        row_type = row['type']
        self.compared[row_type] = self.compared.get(row_type, 0) + 1

    def results(self):
        """Get statistics in a dictionary

        :return:
        """
        results = {
            "Received records": self.collected,
        }
        for key, value in self.compared.items():
            results[f"{key} events"] = value

        return results
