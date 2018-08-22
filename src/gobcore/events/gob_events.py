"""GOB Events

Each possible event in GOB is defined in this module.
The definition includes:
    name - the name of the event, e.g. ADD
    timestamp - the name of the timestamp attribute in the GOB entity that tells when this event has last been applied
    get_modification - method to

todo: The classname is GOBAction and the filename is gob_events. This is confusing.

todo: GOBAction is an abstract base class, why not subclass from ABC?

"""


class GOBAction():
    name = "action"
    timestamp_field = None  # Each action is timestamped

    @classmethod
    def get_modification(self, _source_id, _id_column, _id, **kwargs):
        contents = kwargs['contents'] if 'contents' in kwargs else {}
        contents["_source_id"] = _source_id
        contents[_id_column] = _id
        return {"action": self.name, "contents": contents}


class ADD(GOBAction):
    """
    Example:
        ADD
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        contents: {
            meetboutid: "12881429",
            ....
        }
    """
    name = "ADD"
    timestamp_field = "_date_created"


class MODIFIED(GOBAction):
    """
    Example:
        MODIFIED
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        contents: {
            mutations: [{
                key: "indicatie_beveiligd",
                old_value: "true",
                new_value: "false"
            }]
        }
    """
    name = "MODIFIED"
    timestamp_field = "_date_modified"

    @classmethod
    def get_modification(self, _source_id, _id_column, _id, **kwargs):
        assert 'mutations' in kwargs
        contents = {'mutations': kwargs['mutations']}
        return super().get_modification(_source_id, _id_column, _id, contents=contents)


class DELETED(GOBAction):
    """
    Example:
        DELETED
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        contents: {}
    """
    name = "DELETED"
    timestamp_field = "_date_deleted"


class CONFIRMED(GOBAction):
    """
    Example:
        CONFIRMED
        entity: meetbouten
        source: meetboutengis
        source_id: 12881429
        contents: {}
    """
    name = "CONFIRMED"
    timestamp_field = "_date_confirmed"
