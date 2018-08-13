class GOBAction():
    name = "action"
    timestamp_field = None

    @classmethod
    def get_modification(self, _source_id, _id_column, _id, **kwargs):
        contents = kwargs['contents'] if 'contents' in kwargs else {}
        contents["_source_id"] = _source_id
        contents[_id_column] = _id
        return {"action": self.name, "contents": contents}


class ADD(GOBAction):
    name = "ADD"
    timestamp_field = "_date_created"


class MODIFIED(GOBAction):
    name = "MODIFIED"
    timestamp_field = "_date_modified"

    @classmethod
    def get_modification(self, _source_id, _id_column, _id, **kwargs):
        assert 'mutations' in kwargs
        contents = {'mutations': kwargs[ 'mutations']}
        return super().get_modification(_source_id, _id_column, _id, contents=contents)


class DELETED(GOBAction):
    name = "DELETED"
    timestamp_field = "_date_deleted"


class CONFIRMED(GOBAction):
    name = "CONFIRMED"
    timestamp_field = "_date_confirmed"
