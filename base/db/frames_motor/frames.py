import ast

from django.core.exceptions import ValidationError
from contextlib import contextmanager

# from blinker import signal
from bson.objectid import ObjectId
from datetime import date, datetime, timezone

from base.db.fields import ObjectIdField, ForeignFrame, NOT_PROVIDED, Field, ArrayField, EmbeddedField, ForeignKey, \
    UTC_NOW, AUTO_NOW, DateField, DateTimeField
from base.db.frames_motor.queries import to_refs, Condition, Group

__all__ = [
    'Frame',
    'SubFrame',
    'RESTRICT',
    'CASCADE',
    'SET_NULL',
    'SET_DEFAULT'
]

from base.rf.exceptions import FrameValidation

CASCADE = '_cascade'
RESTRICT = '_restrict'
SET_NULL = '_set_null'
SET_DEFAULT = '_set_default'


class _BaseFrame:
    """
    Base class for Frames and SubFrames.
    """

    # A cache of key lists generated from path strings used for performance (see
    # `_path_to_keys`.
    include = list()
    exclude = list()
    _meta = {}

    def __init__(self, *args, **kwargs):
        self._update_field = set()
        self.errors = list()
        self._meta = dict()
        self._child_frames = dict()
        self.additional = list()
        for key, value in self.__class__.__dict__.items():
            if isinstance(value, Field):
                self._meta[key] = value
                if value.default == NOT_PROVIDED:
                    value = None
                elif value.default == UTC_NOW:
                    value = datetime.utcnow()
                elif value.default == AUTO_NOW:
                    value = datetime.now()
                else:
                    value = value.default
                self[key] = value
            elif isinstance(value, ForeignFrame):
                self._child_frames[key] = value
        self._update_field.clear()
        if args and isinstance(args[0], dict):
            self.set_items(args[0].items())
        if kwargs:
            if kwargs.keys().__contains__('data'):
                data = kwargs.pop('data')
                self.set_items(data)
            self.set_items(kwargs)

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __setattr__(self, key, value):
        super(_BaseFrame, self).__setattr__(key, value)
        if key in self._meta.keys():
            self._update_field.add(key)

    def set_items(self, dictionary):
        if isinstance(dictionary, dict):
            dictionary = dictionary.items()
        for key, value in dictionary:
            if key in self._child_frames.keys():
                if value:
                    pass
                    # self[key] = self._child_frames[key].frame(value)
            else:
                map = self._meta.get(key)
                if map:
                    if isinstance(map, EmbeddedField):
                        setattr(self, key, map.to(value))
                        continue
                    if isinstance(map, ArrayField):
                        if isinstance(map.to, EmbeddedField):
                            obj = map.to.to
                            value = [obj(val) for val in value]
                            setattr(self, key, value)
                            continue
                setattr(self, key, value)

    def get(self, name, default=None):
        return self.__dict__.get(name, default)

    def _get_document_value(self, cls, value):
        if isinstance(cls, ArrayField):
            ret_val = [self._get_document_value(cls.to, item) for item in value]
        elif isinstance(cls, EmbeddedField):
            ret_val = value._get_document()
        elif isinstance(cls, DateField) and not isinstance(cls, DateTimeField):
            ret_val = datetime(value.year, value.month, value.day)
        else:
            ret_val = value
        return ret_val

    def _get_document(self):
        document = dict()
        valid_keys = self._update_field if self._update_field else self._meta.keys()
        for key in self._meta.keys():
            if key in valid_keys:
                if self[key]:
                    document[key] = self._get_document_value(self._meta[key], self[key])
                else:
                    document[key] = self[key]
        return document

    # Serializing

    def is_valid(self, raise_exceptions=True):
        if not (getattr(self, '_id', None) and '_id' in self._meta.keys()):
            validate_fields = self._meta.keys()
        elif self._update_field:
            validate_fields = self._update_field
        else:
            validate_fields = self._meta.keys()
        for key, value in self._meta.items():
            if key in validate_fields:
                try:
                    cleaned = value.clean(self[key])
                    self[key] = cleaned
                except ValidationError as e:
                    er = {key: e.messages[0]}
                    self.errors.append(er)
                except FrameValidation as e:
                    er = {key: e.message}
                    self.errors.append(er)
        if self.errors and raise_exceptions:
            raise FrameValidation(self.errors)
        return self

    def clean(self, value):
        if isinstance(value, _BaseFrame):
            if value.is_valid():
                return value

    def clear_update_fields(self):
        self._update_field.clear()

    def to_json_type(self):
        """
        Return a dictionary for the document with values converted to JSON safe
        types.
        """
        temp = list(self._meta.keys())
        items = list()
        if self.include:
            for key in temp:
                if key in self.include:
                    items.append(key)
            temp = items
        elif self.exclude:
            for key in self.exclude:
                try:
                    temp.remove(key)
                except KeyError:
                    pass
        temp.extend(self.additional)
        result = dict()
        for key in temp:
            result.update({key: self._json_safe(self[key])})
        return result

    @classmethod
    def _json_safe(cls, value):
        """Return a JSON safe value"""
        # Date
        if type(value) == date:
            return str(value)

        # Datetime
        elif type(value) == datetime:
            return str(value)

        # Object Id
        elif isinstance(value, ObjectId):
            return str(value)

        # Frame
        elif isinstance(value, _BaseFrame):
            return value.to_json_type()

        # Lists
        elif isinstance(value, (list, tuple)):
            return [cls._json_safe(v) for v in value]

        # Dictionaries
        elif isinstance(value, dict):
            return {k: cls._json_safe(v) for k, v in value.items()}

        return value


class _FrameMeta(type):
    """
    Meta class for `Frame`s to ensure an `_id` is present in any defined set of
    fields.
    """

    def __new__(meta, name, bases, dct):
        # If a set of fields is defined ensure it contains `_id`
        # If no collection name is set then use the class name
        if dct.get('_collection') is None:
            dct['_collection'] = name

        if dct.get('_id') is None:
            dct['_id'] = ObjectIdField(null=True)
        return super(_FrameMeta, meta).__new__(meta, name, bases, dct)


class Frame(_BaseFrame, metaclass=_FrameMeta):
    """
    Frames allow documents to be wrapped in a class adding support for dot
    notation access to attributes and numerous short-cut/helper methods.
    """

    # The MongoDB client used to interface with the database

    _client = None
    # The database on which this collection the class represents is located
    _db = None

    # The database collection this class represents
    _collection = None

    # def __init__(self, *args, **kwargs):
    #     super(Frame, self).__init__(*args, **kwargs)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._id == other._id

    def __hash__(self):
        if not self._id:
            raise TypeError('Cannot hash a document without an `_id` set.')
        return int(str(self._id), 16)

    def __lt__(self, other):
        return self._id < other._id

    # Operations

    async def save(self):
        """Insert or Update document"""
        if self["_id"] is None:
            return await self.insert()
        else:
            return await self.update()

    async def insert(self):
        """Insert this document"""
        # Send insert signal
        # signal('insert1').send(self.__class__, frames=[self])
        # Prepare the document to be inserted
        self._update_field.clear()
        self.is_valid()

        document = self._get_document()
        if self._id is None:
            document.pop('_id')
        # validate data

        # TODO -> IMPLEMENT SAGA

        # Insert the document and update the Id
        inserted_field = await self.get_collection().insert_one(document)
        if inserted_field.inserted_id:
            self._id = inserted_field.inserted_id
            return True
        return False
        # Send inserted signal
        # signal('inserted').send(self.__class__, frames=[self])

    async def unset(self, *fields):
        """Unset the given list of fields for this document."""

        # Send update signal
        # signal('update').send(self.__class__, frames=[self])

        # Clear the fields from the document and build the unset object
        unset = {}
        for field in fields:
            unset[field] = True
            self[field] = None

        # Update the document
        self.get_collection().update_one(
            {'_id': self._id},
            {'$unset': unset}
        )

        # Send updated signal
        # signal('updated').send(self.__class__, frames=[self])

    async def update(self):
        """
        Update this document. Optionally a specific list of fields to update can
        be specified.
        """

        # assert '_id' in self.__dict__, "Can't update documents without `_id`"

        # Send update signal
        # signal('update').send(self.__class__, frames=[self])

        # Check for selective updates
        self.is_valid()
        document = self._get_document()
        # Update the document_
        if not isinstance(self._id, ObjectId):
            self._id = ObjectId(self._id)
        if document == {}:
            return False
        update_result = await self.get_collection().update_one({'_id': self._id}, {'$set': document})
        return update_result.matched_count + update_result.modified_count

        # Send updated signal
        # signal('updated').send(self.__class__, frames=[self])

    async def delete(self):
        """Delete this document"""
        if self._child_frames:
            for value in self._child_frames.values():
                if not await getattr(self, value.on_delete, None)(value):
                    return False
        delete_res = await self.get_collection().delete_one({"_id": ObjectId(self._id)})
        if delete_res.deleted_count >= 1:
            return True
        return False

    async def _get_parent_key(self, frame):
        for key, value in self._meta.items():
            if isinstance(value, ForeignKey):
                if isinstance(value.to, str):
                    str_frame = frame.__class__.__name__
                else:
                    str_frame = frame
                if value.to == str_frame:
                    return key, False, value.default
            if isinstance(value, ArrayField):
                if isinstance(value.to, ForeignKey):
                    if isinstance(value.to, str):
                        str_frame = frame.__class__.__name__
                    else:
                        str_frame = frame
                    if value.to == str_frame:
                        return key, True, value.default

    async def _cascade(self, value=None):
        """Delete multiple documents"""
        child = value.frame
        pr_key, array, default_value = await child()._get_parent_key(self)
        if array:
            children = await child.many({pr_key: ObjectId(self._id)})
            for ch in children:
                await ch.update({pr_key: ObjectId(self._id)}, {"$pull": {pr_key: ObjectId(self._id)}})
        else:
            child_class = child()
            if child_class._child_frames:
                children = await child.many({pr_key: ObjectId(self._id)})
                for ch in children:
                    ch.delete_without_trans()
            else:
                await child.get_collection().delete_many({pr_key: ObjectId(self._id)})
        return True

    async def _restrict(self, value=None):
        """Not delete a document if its have a child"""
        child = value.frame
        pr_key, array, default_value = await child()._get_parent_key(self)
        if array:
            children = await child.many({pr_key: ObjectId(self._id)})
            if children:
                return False
        else:
            child_class = child()
            if child_class._child_frames:
                children = await child.many({pr_key: ObjectId(self._id)})
                for ch in children:
                    ch.delete_without_trans()
            else:
                return False
        return True

    async def _set_null(self, value=None):
        """set null for all child of document"""
        child = value.frame
        pr_key, array, default_value = await child()._get_parent_key(self)
        if array:
            children = await child.many({pr_key: ObjectId(self._id)})
            for ch in children:
                await ch.update({pr_key: ObjectId(self._id)}, {"$pull": {pr_key: ObjectId(self._id)}})
        else:
            child_class = child()
            if child_class._child_frames:
                children = await child.many({pr_key: ObjectId(self._id)})
                for ch in children:
                    ch.delete_without_trans()
            else:
                await child.get_collection().update_many({pr_key: ObjectId(self._id)},
                                                         {"$set": {pr_key: None}})
        return True

    async def _set_default(self, value=None):
        """set default value for all child of document"""
        child = value.frame
        pr_key, array, default_value = await child()._get_parent_key(self)
        if array:
            children = await child.many({pr_key: ObjectId(self._id)})
            for ch in children:
                await ch.update({pr_key: ObjectId(self._id)}, {"$pull": {pr_key: ObjectId(self._id)}})
        else:
            child_class = child()
            if child_class._child_frames:
                children = await child.many({pr_key: ObjectId(self._id)})
                for ch in children:
                    ch.delete_without_trans()
            else:
                await child.get_collection().update_many({pr_key: ObjectId(self._id)},
                                                         {"$set": {pr_key: default_value}})
        return True

    async def pull(self, pull_condition):
        update_result = await self.get_collection().update_one({'_id': self._id}, {'$pull': pull_condition})
        return update_result.matched_count + update_result.modified_count

    async def push(self, key, document):
        document._update_field.clear()
        document = document._get_document()
        update_result = await self.get_collection().update_one({'_id': self._id}, {'$push': {key: document}})
        return update_result.matched_count + update_result.modified_count

    @classmethod
    async def raw_update_one(cls, filter, update, **kwargs):
        update_result = await cls.get_collection().update_one(filter, update, **kwargs)
        return update_result.matched_count + update_result.modified_count

    @classmethod
    async def raw_update_many(cls, filter, update, **kwargs):
        update_result = await cls.get_collection().update_many(filter, update, **kwargs)
        return update_result.matched_count + update_result.modified_count

    @classmethod
    async def one(cls, filter=None, **kwargs):
        """Return the first document matching the filter"""
        if kwargs:
            document = await cls.get_collection().find_one(filter, kwargs)
        else:
            document = await cls.get_collection().find_one(filter)

        # Make sure we found a document
        if not document:
            return
        doc = cls(document)
        doc.clear_update_fields()
        return doc

    @classmethod
    async def one_json(cls, filter=None, **kwargs):
        """Return the first document matching the filter"""

        if kwargs:
            document = await cls.get_collection().find_one(filter, kwargs)
        else:
            document = await cls.get_collection().find_one(filter)

        # Make sure we found a document
        if not document:
            return
        return cls(document).to_json_type()

    @classmethod
    async def one_no_cast(cls, filter=None, **kwargs):
        """Return the first document matching the filter without casting to frame"""
        if kwargs:
            document = await cls.get_collection().find_one(filter, kwargs)
        else:
            document = await cls.get_collection().find_one(filter)

        # Make sure we found a document
        if not document:
            return
        return document

    @classmethod
    async def many(cls, filter=None, **kwargs):
        """Return a list of documents matching the filter"""

        if kwargs:
            documents = cls.get_collection().find(filter, kwargs)
        else:
            documents = cls.get_collection().find(filter)

        if documents is None:
            return None

        doc = []
        async for d in documents:
            doc.append(cls(d))
        return doc

    @classmethod
    async def many_json(cls, filter=None, **kwargs):
        """Return a list of documents matching the filter"""

        if kwargs:
            documents = cls.get_collection().find(filter, kwargs)
        else:
            documents = cls.get_collection().find(filter)

        if documents is None:
            return None

        doc = []
        async for d in documents:
            doc.append(cls(d).to_json_type())
        return doc

    @classmethod
    async def many_no_cast(cls, filter=None, **kwargs):
        """Return a list of documents matching the filter"""
        if kwargs:
            documents = cls.get_collection().find(filter, kwargs)
        else:
            documents = cls.get_collection().find(filter)

        if documents is None:
            return None

        doc = []
        async for d in documents:
            doc.append(d)
        return doc

    @classmethod
    async def aggregate(cls, pipeline):
        additional = list()
        for item in pipeline:
            proj = item.get('$project', None)
            if proj:
                additional.extend(list(proj.keys()))
        documents = cls.get_collection().aggregate(pipeline)
        # if documents in None:
        #     return
        doc = []
        instance = cls()
        for key in instance._meta.keys():
            if key in additional:
                additional.remove(key)
        async for d in documents:
            res = cls(d)
            res.additional = additional
            doc.append(res)
        return doc

    @classmethod
    async def aggregate_json(cls, pipeline):
        additional = list()
        for item in pipeline:
            proj = item.get('$project', None)
            if proj:
                additional.extend(list(proj.keys()))
        documents = cls.get_collection().aggregate(pipeline)
        # if documents in None:
        #     return
        doc = []
        instance = cls()
        for key in instance._meta.keys():
            if key in additional:
                additional.remove(key)
        async for d in documents:
            res = cls(d)
            res.additional = additional
            doc.append(res.to_json_type())
        return doc

    @classmethod
    async def aggregate_no_cast(cls, pipeline):
        documents = cls.get_collection().aggregate(pipeline)
        # if documents in None:
        #     return
        doc = []
        async for d in documents:
            doc.append(d)
        return doc

    @classmethod
    async def counts(cls, pipeline):
        pipeline.append({"$count": "count"})
        documents = cls.get_collection().aggregate(pipeline)
        async for d in documents:
            return d.get("count")
        # result = await documents

    @classmethod
    async def insert_many(cls, documents, ordered=True):
        """Insert a list of documents"""
        error_list = list()
        list_of_frames = list()
        if isinstance(documents, list) and documents[0]:
            if isinstance(documents[0], Frame):
                for doc in documents:
                    d = doc._get_document()
                    if doc._id is None:
                        d.pop('_id')
                    list_of_frames.append(d)
            else:
                for index, doc in enumerate(documents):
                    try:
                        frame = cls(doc).is_valid()
                    except ValidationError as e:
                        error_list.append({"index": index, "errors": ast.literal_eval(e.message)})
                        continue
                    if frame._id is None:
                        dd = frame._get_document()
                        dd.pop('_id')
                        list_of_frames.append(dd)
                    else:
                        list_of_frames.append(frame._get_document())
        if error_list:
            raise ValidationError(message=str(error_list))
        inserted_ids = await cls.get_collection().insert_many(list_of_frames, ordered=ordered)
        return True

    #
    #     # Ensure all documents have been converted to frames
    #     # frames = await self._ensure_frames(documents)
    #
    #     # Send insert signal
    #     # signal('insert').send(cls, frames=frames)
    #
    #     # Prepare the documents to be inserted
    #
    #     self._update_field.clear()
    #     self.is_valid()
    #
    #     # documents = [to_refs(f.__dict__) for f in frames]
    #     # Bulk insert
    #     ids = await self.get_collection().insert_many(documents)
    #
    #     # Apply the Ids to the frames
    #     for i, id in enumerate(ids):
    #         frames[i]._id = id
    #
    #     # Send inserted signal
    #     # signal('inserted').send(cls, frames=frames)
    #
    #     # return frames
    #
    # @classmethod
    # async def update_many(cls, documents, *fields):
    #     """
    #     Update multiple documents. Optionally a specific list of fields to
    #     update can be specified.
    #     """
    #
    #     # Ensure all documents have been converted to frames
    #     frames = cls._ensure_frames(documents)
    #
    #     all_count = len(documents)
    #     assert len([f for f in frames if '_id' in f.__dict__]) == all_count, \
    #         "Can't update documents without `_id`s"
    #
    #     # Send update signal
    #     signal('update').send(cls, frames=frames)
    #
    #     # Prepare the documents to be updated
    #
    #     # Check for selective updates
    #     if len(fields) > 0:
    #         documents = []
    #         for frame in frames:
    #             document = {'_id': frame._id}
    #             for field in fields:
    #                 document[field] = cls._path_to_value(
    #                     field,
    #                     frame.__dict__
    #                 )
    #             documents.append(to_refs(document))
    #     else:
    #         documents = [to_refs(f.__dict__) for f in frames]
    #
    #     # Update the documents
    #     requests = []
    #     for document in documents:
    #         _id = document.pop('_id')
    #         requests.append(UpdateOne({'_id': _id}, {'$set': document}))
    #
    #     cls.get_collection().bulk_write(requests)
    #
    #     # Send updated signal
    #     signal('updated').send(cls, frames=frames)

    # @classmethod
    # async def unset_many(cls, documents, *fields):
    #     """Unset the given list of fields for this document."""
    #
    #     # Ensure all documents have been converted to frames
    #     frames = cls._ensure_frames(documents)
    #
    #     all_count = len(documents)
    #     assert len([f for f in frames if '_id' in f.__dict__]) == all_count, \
    #         "Can't update documents without `_id`s"
    #
    #     # Send update signal
    #     # signal('update').send(cls, frames=frames)
    #
    #     # Build the unset object
    #     unset = {}
    #     for field in fields:
    #         unset[field] = True
    #
    #     # Clear the fields from the documents and build a list of ids to
    #     # update.
    #     ids = []
    #     for document in documents:
    #         ids.append(document._id)
    #         for frame in frames:
    #             frame.__dict__.pop(field, None)
    #
    #     # Update the document
    #     cls.get_collection().update_many(
    #         {'_id': {'$in': ids}},
    #         {'$unset': unset}
    #     )

    # Send updated signal
    # signal('updated').send(cls, frames=frames)

    @classmethod
    async def delete_many(cls, key, id):
        """Delete multiple documents"""
        if not isinstance(id, ObjectId):
            id = ObjectId(id)
        await cls.get_collection().delete_many({key: ObjectId(id)})

        return True

    @classmethod
    async def raw_delete_many(cls, filter, **kwargs):
        """Delete multiple documents"""
        await cls.get_collection().delete_many(filter, **kwargs)

        return True

    @classmethod
    async def raw_delete_one(cls, filter, **kwargs):
        """Delete multiple documents"""
        await cls.get_collection().delete_one(filter, **kwargs)

        return True

    # @classmethod
    # async def _ensure_frames(cls, documents):
    #     """
    #     Ensure all items in a list are frames by converting those that aren't.
    #     """
    #     frames = []
    #     for document in documents:
    #         if not isinstance(document, Frame):
    #             frames.append(cls(document))
    #         else:
    #             frames.append(document)
    #     return frames

    # Querying

    async def reload(self, **kwargs):
        """Reload the document"""
        return self.one({'_id': self._id}, **kwargs)

    @classmethod
    async def count_by_filter(cls, filter=None, **kwargs):
        """Return a count of documents matching the filter"""

        if isinstance(filter, (Condition, Group)):
            filter = filter.to_dict()

        filter = to_refs(filter)

        if filter:
            return cls.get_collection().count_documents(
                to_refs(filter),
                **kwargs
            )
        else:
            return cls.get_collection().estimated_document_count(**kwargs)

    @classmethod
    async def ids(cls, filter, **kwargs):
        """Return a list of Ids for documents matching the filter"""
        documents = cls.get_collection().find(
            filter,
            projection={'_id': 1},
            **kwargs
        )
        return [d['_id'] async for d in documents]

    @classmethod
    async def nullify(cls, filter, fields):
        """Nullify a reference field (does not emit signals)"""
        await cls.get_collection().update_many(
            filter,
            {'$set': {{field: None} for field in fields}}
        )

    # Signals

    @classmethod
    def listen(cls, event, func):
        """Add a callback for a signal against the class"""
        # signal(event).connect(func, sender=cls)

    @classmethod
    def stop_listening(cls, event, func):
        """Remove a callback for a signal against the class"""
        # signal(event).disconnect(func, sender=cls)

    # Misc.

    @classmethod
    def get_collection(cls):
        """Return a reference to the database collection for the class"""
        return getattr(
            cls,
            '_collection_context',
            getattr(cls.get_db(), cls._collection)
        )

    @classmethod
    def get_db(cls):
        """Return the database for the collection"""
        if cls._db:
            return getattr(cls._client, cls._db)
        return cls._client.get_default_database()

    @classmethod
    @contextmanager
    def with_options(cls, **options):
        existing_context = getattr(cls, '_collection_context', None)

        try:
            collection = getattr(cls.get_db(), cls._collection)
            cls._collection_context = collection.with_options(**options)
            yield cls._collection_context

        finally:
            if existing_context is None:
                del cls._collection_context

            else:
                cls._collection_context = existing_context


class SubFrame(_BaseFrame):
    """
    Sub-frames allow embedded documents to be wrapped in a class adding support
    for dot notation access to attributes.
    """
