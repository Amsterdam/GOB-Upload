import json
import argparse
import os
import shutil
import zipfile

from gobcore.events.import_events import ImportEvent
from gobcore.model import GOBModel
from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.event_applicator import _get_gob_event


class EventsFileWriter:
    """EventsFileWriter

    """
    EXPORT_DIR = 'gobevents'

    def __init__(self, catalog: str, collection: str = None, zip: bool = False):
        self.catalog = catalog
        self.collection = collection
        self.zip = zip

    def _db_to_gob_event(self, event) -> ImportEvent:
        # Parse the json data of the event
        if isinstance(event.contents, dict):
            data = event.contents
        else:
            data = json.loads(event.contents)

        return _get_gob_event(event, data)

    def _write_events(self, catalog: str, collection: str, dst_dir: str):
        filename = f"{dst_dir}/{catalog}_{collection}.gobevents"
        storage = GOBStorageHandler()

        print(f"Requesting events for {catalog} {collection}")
        with storage.get_session() as session:
            events = session \
                .query(storage.DbEvent) \
                .yield_per(10000) \
                .filter_by(catalogue=catalog, entity=collection) \
                .order_by(storage.DbEvent.eventid.asc())

            with open(filename, 'w') as f:
                print(f"Writing events for {catalog} {collection} to {filename}")

                for event in events:
                    gob_event = self._db_to_gob_event(event)
                    data = {
                        '_event_type': gob_event.name,
                        '_event_id': gob_event.id,
                        **gob_event._data,
                    }
                    f.write(f"{data['_source_id']}|{json.dumps(data)}\n")
                print(f"Done writing events")

    def _zipdir(self, path: str, filename: str):
        zipf = zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED)

        for root, _, files in os.walk(path):
            for file in files:
                zipf.write(os.path.join(root, file))

        zipf.close()

    def write(self):
        dst_dir = f"{self.EXPORT_DIR}/{self.catalog}"
        try:
            shutil.rmtree(dst_dir)
        except FileNotFoundError:
            pass
        os.makedirs(dst_dir, exist_ok=True)

        if self.collection:
            self._write_events(self.catalog, self.collection, dst_dir)
        else:
            for collection in GOBModel().get_collection_names(self.catalog):
                self._write_events(self.catalog, collection, dst_dir)

        print(f"Done exporting {self.catalog} {self.collection if self.collection else ''}")

        if self.zip:
            zip_filename = f"{self.EXPORT_DIR}/{self.catalog}.zip"
            self._zipdir(dst_dir, zip_filename)
            print(f"Zipped results to {zip_filename}")


def main():
    parser = argparse.ArgumentParser(description='Export GOB events to file')
    parser.add_argument('catalog', type=str, help='The catalog to export')
    parser.add_argument('collection',
                        type=str,
                        nargs='?',
                        help='The collection to export. Exports all collections if omitted')
    parser.add_argument('--zip', dest='zip', action='store_true', help='Zip exported files?')

    args = parser.parse_args()
    writer = EventsFileWriter(args.catalog, args.collection, args.zip)
    writer.write()


if __name__ == "__main__":
    main()
