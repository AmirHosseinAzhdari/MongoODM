import ast

from bson import json_util
from django.core.exceptions import ValidationError
from contextlib import contextmanager

# from blinker import signal
from bson.objectid import ObjectId
from copy import deepcopy
from datetime import date, datetime, timezone

from base.db.fields import ObjectIdField, ForeignFrame, NOT_PROVIDED, Field, ArrayField, EmbeddedField, ForeignKey
from base.db.frames_motor.queries import to_refs, Condition, Group
from motor import motor_asyncio

__all__ = [
    'Frame',
    'SubFrame',
    'RESTRICT',
    'CASCADE',
    'SET_NULL',
    'SET_DEFAULT'
]

CASCADE = 'cascade'
RESTRICT = 'restrict'
SET_NULL = 'set_null'
SET_DEFAULT = 'set_default'


class _BaseFrame:
    """
    Base class for Frames and SubFrames.
    """

    # A cache of key lists generated from path strings used for performance (see
    # `_path_to_keys`.
    _path_to_keys_cache = {}
    include = list()
    exclude = list()
    _meta = {}
    errors = list()

    def __init__(self, *args, **kwargs):
        self._update_field = set()
        self.errors = list()
        self._document = dict()
        self._meta = dict()
        self._child_frames = dict()
        for key, value in self.__class__.__dict__.items():
            if isinstance(value, Field):
                self._meta[key] = value
                if value.default == NOT_PROVIDED:
                    value = None
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
                setattr(self, key, value)

    def get(self, name, default=None):
        return self.__dict__.get(name, default)

    def _get_document(self):
        document = dict()
        valid_keys = self._update_field if self._update_field else self._meta.keys()
        for key in self._meta.keys():
            if key in valid_keys:

                if isinstance(self._meta[key], ArrayField):
                    value = list()
                    for item in self[key]:
                        if isinstance(item, _BaseFrame):
                            value.append(item._get_document())
                        else:
                            value.append(item)
                    document.update({key: value})
                elif isinstance(self._meta[key], EmbeddedField):
                    document.update({key: self[key]._get_document()})
                else:
                    document.update({key: self[key]})
        return document

    # Serializing

    def is_valid(self):
        if not (getattr(self, '_id', None) and '_id' in self._meta.keys()):
            self._update_field.clear()
        if self._update_field:
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
        if self.errors:
            raise ValidationError(message=str(self.errors))
        return self

    def clean(self, value):
        if isinstance(value, _BaseFrame):
            if value.is_valid():
                return value

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


@classmethod
def _path_to_keys(cls, path):
    """Return a list of keys for a given path"""

    # Paths are cached for performance
    keys = _BaseFrame._path_to_keys_cache.get(path)
    if keys is None:
        keys = _BaseFrame._path_to_keys_cache[path] = path.split('.')

    return keys


@classmethod
def _path_to_value(cls, path, parent_dict):
    """Return a value from a dictionary at the given path"""
    keys = cls._path_to_keys(path)

    # Traverse to the tip of the path
    child_dict = parent_dict
    for key in keys[:-1]:
        child_dict = child_dict.get(key)
        if child_dict is None:
            return

    return child_dict.get(keys[-1])


@classmethod
def _remove_keys(cls, parent_dict, paths):
    """
    Remove a list of keys from a dictionary.

    Keys are specified as a series of `.` separated paths for keys in child
    dictionaries, e.g 'parent_key.child_key.grandchild_key'.
    """
    for path in paths:
        keys = cls._path_to_keys(path)

        # Traverse to the tip of the path
        child_dict = parent_dict
        for key in keys[:-1]:
            child_dict = child_dict.get(key)

            if child_dict is None:
                break

        if child_dict is None:
            continue

        # Remove the key
        if keys[-1] in child_dict:
            child_dict.pop(keys[-1])


# Public methods

@classmethod
def get_fields(cls):
    """Return the set of fields defined for the class"""
    return set(cls._fields)


@classmethod
def get_private_fields(cls):
    """Return the set of private fields defined for the class"""
    return set(cls._private_fields)


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

    # client = motor_asyncio.AsyncIOMotorClient(
    #     "mongodb://root:example@10.10.10.20:27018/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false")
    _client = None
    # The database on which this collection the class represents is located
    _db = None

    # The database collection this class represents
    _collection = None

    # The documents defined fields
    _fields = set()

    # A set of private fields that will be excluded from the output of
    # `to_json_type`.
    _private_fields = set()

    # Default projection
    _default_projection = None

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
            self.__dict__.pop(field, None)
            unset[field] = True

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

    async def upsert(self):
        """
        Update or Insert this document depending on whether it exists or not.
        The presense of an `_id` value in the document is used to determine if
        the document exists.

        NOTE: This method is not the same as specifying the `upsert` flag when
        calling MongoDB. When called for a document with an `_id` value, this
        method will call the database to see if a record with that Id exists,
        if not it will call `insert`, if so it will call `update`. This
        operation is therefore not atomic and much slower than the equivalent
        MongoDB operation (due to the extra call).
        """

        # If no `_id` is provided then we insert the document
        if not self._id:
            return self.insert()

        # If an `_id` is provided then we need to check if it exists before
        # performing the `upsert`.
        #
        if self.count({'_id': self._id}) == 0:
            await self.insert()
        else:
            await self.update()

    async def delete(self):
        """Delete this document"""
        if self._child_frames:
            for value in self._child_frames.values():
                if value.on_delete == RESTRICT:
                    if not await getattr(self, value.on_delete, None)(value):
                        return False
            for value in self._child_frames.values():
                if not value.on_delete == RESTRICT:
                    if not await getattr(self, value.on_delete, None)(value):
                        return False
        delete_res = await self.get_collection().delete_one({"_id": ObjectId(self._id)})
        if delete_res.deleted_count >= 1:
            return True
        return False

    async def get_key(self, frame):
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

    async def cascade(self, value=None):
        """Delete multiple documents"""
        child = value.frame
        pr_key, array, default_value = await child().get_key(self)
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

    async def restrict(self, value=None):
        """Not delete a document if its have a child"""
        child = value.frame
        pr_key, array, default_value = await child().get_key(self)
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

    async def set_null(self, value=None):
        """set null for all child of document"""
        child = value.frame
        pr_key, array, default_value = await child().get_key(self)
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

    async def set_default(self, value=None):
        """set default value for all child of document"""
        child = value.frame
        pr_key, array, default_value = await child().get_key(self)
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

    @classmethod
    async def aggregate(cls, pipeline):
        documents = cls.get_collection().aggregate(pipeline)
        # if documents in None:
        #     return
        doc = []
        async for d in documents:
            doc.append(cls(d).is_valid())
        return doc

    @classmethod
    async def counts(cls, pipeline):
        pipeline.append({"$count": "count"})
        documents = cls.get_collection().aggregate(pipeline)
        async for d in documents:
            return d.get("count")
        # result = await documents

    @classmethod
    async def insert_many(cls, documents):
        """Insert a list of documents"""
        error_list = list()
        list_of_frames = list()
        if isinstance(documents, list) and documents[0]:
            if isinstance(documents[0], _BaseFrame):
                for doc in documents:
                    if doc._id is None:
                        doc._document.pop('_id')
                    list_of_frames.append(doc._get_document())
            else:
                for index, doc in enumerate(documents):
                    frame = cls(doc)
                    try:
                        frame.is_valid()
                    except Exception as e:
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
        inserted_ids = await cls.get_collection().insert_many(list_of_frames)
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

    @classmethod
    async def unset_many(cls, documents, *fields):
        """Unset the given list of fields for this document."""

        # Ensure all documents have been converted to frames
        frames = cls._ensure_frames(documents)

        all_count = len(documents)
        assert len([f for f in frames if '_id' in f.__dict__]) == all_count, \
            "Can't update documents without `_id`s"

        # Send update signal
        # signal('update').send(cls, frames=frames)

        # Build the unset object
        unset = {}
        for field in fields:
            unset[field] = True

        # Clear the fields from the documents and build a list of ids to
        # update.
        ids = []
        for document in documents:
            ids.append(document._id)
            for frame in frames:
                frame.__dict__.pop(field, None)

        # Update the document
        cls.get_collection().update_many(
            {'_id': {'$in': ids}},
            {'$unset': unset}
        )

        # Send updated signal
        # signal('updated').send(cls, frames=frames)

    @classmethod
    async def delete_many(cls, key, id):
        """Delete multiple documents"""
        if not isinstance(id, ObjectId):
            id = ObjectId(id)
        await cls.get_collection().delete_many({key: ObjectId(id)})

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
        frame = self.one({'_id': self._id}, **kwargs)
        self.__dict__ = frame.__dict__

    # @classmethod
    # async def by_id(cls, id, **kwargs):
    #     """Get a document by ID"""
    #     return cls.one({'_id': id}, **kwargs)

    @classmethod
    async def count(cls, filter=None, **kwargs):
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
    async def ids(cls, filter=None, **kwargs):
        """Return a list of Ids for documents matching the filter"""

        # Find the documents
        if isinstance(filter, (Condition, Group)):
            filter = filter.to_dict()

        documents = cls.get_collection().find(
            to_refs(filter),
            projection={'_id': True},
            **kwargs
        )

        return [d['_id'] for d in list(documents)]

    @classmethod
    async def one(cls, filter=None, **kwargs):
        """Return the first document matching the filter"""

        # Flatten the projection
        # kwargs['projection'], references, subs = \
        #     cls._flatten_projection(
        #         kwargs.get('projection', cls._default_projection)
        #     )

        # Find the document
        # if isinstance(filter, (Condition, Group)):
        #     filter = filter.to_dict()

        # for key, value in filter:
        #     filter[key] = to_refs(value)

        if kwargs:
            document = await cls.get_collection().find_one(filter, kwargs)
        else:
            document = await cls.get_collection().find_one(filter)

        # Make sure we found a document
        if not document:
            return
        return cls(document).is_valid()

    @classmethod
    async def many(cls, filter=None, **kwargs):
        """Return a list of documents matching the filter"""

        # Flatten the projection
        # kwargs['projection'], references, subs = \
        #     cls._flatten_projection(
        #         kwargs.get('projection', cls._default_projection)
        #     )

        # Find the documents
        # if isinstance(filter, (Condition, Group)):
        #     filter = filter.to_dict()

        if kwargs:
            documents = cls.get_collection().find(filter, kwargs)
        else:
            documents = cls.get_collection().find(filter)

        # Dereference the documents (if required)
        # if references:
        #     cls._dereference(documents, references)

        # Add sub-frames to the documents (if required)
        # if subs:
        #     cls._apply_sub_frames(documents, subs)
        if documents is None:
            return

        doc = []
        async for d in documents:
            doc.append(cls(d).is_valid())
        return doc

    @classmethod
    def _apply_sub_frames(cls, documents, subs):
        """Convert embedded documents to sub-frames for one or more documents"""

        # Dereference each reference
        for path, projection in subs.items():

            # Get the SubFrame class we'll use to wrap the embedded document
            sub = None
            expect_map = False
            if '$sub' in projection:
                sub = projection.pop('$sub')
            elif '$sub.' in projection:
                sub = projection.pop('$sub.')
                expect_map = True
            else:
                continue

            # Add sub-frames to the documents
            raw_subs = []
            for document in documents:
                value = cls._path_to_value(path, document)
                if value is None:
                    continue

                if isinstance(value, dict):
                    if expect_map:
                        # Dictionary of embedded documents
                        raw_subs += value.values()
                        for k, v in value.items():
                            if isinstance(v, list):
                                value[k] = [
                                    sub(u) for u in v if isinstance(u, dict)]
                            else:
                                value[k] = sub(v)

                    # Single embedded document
                    else:
                        raw_subs.append(value)
                        value = sub(value)

                elif isinstance(value, list):
                    # List of embedded documents
                    raw_subs += value
                    value = [sub(v) for v in value if isinstance(v, dict)]

                else:
                    raise TypeError('Not a supported sub-frame type')

                child_document = document
                keys = cls._path_to_keys(path)
                for key in keys[:-1]:
                    child_document = child_document[key]
                child_document[keys[-1]] = value

            # Apply the projection to the list of sub frames
            if projection:
                sub._apply_projection(raw_subs, projection)

    @classmethod
    def _dereference(cls, documents, references):
        """Dereference one or more documents"""

        # Dereference each reference
        for path, projection in references.items():

            # Check there is a $ref in the projection, else skip it
            if '$ref' not in projection:
                continue

            # Collect Ids of documents to dereference
            ids = set()
            for document in documents:
                value = cls._path_to_value(path, document)
                if not value:
                    continue

                if isinstance(value, list):
                    ids.update(value)

                elif isinstance(value, dict):
                    ids.update(value.values())

                else:
                    ids.add(value)

            # Find the referenced documents
            ref = projection.pop('$ref')

            frames = ref.many(
                {'_id': {'$in': list(ids)}},
                projection=projection
            )
            frames = {f._id: f for f in frames}

            # Add dereferenced frames to the document
            for document in documents:
                value = cls._path_to_value(path, document)
                if not value:
                    continue

                if isinstance(value, list):
                    # List of references
                    value = [frames[id] for id in value if id in frames]

                elif isinstance(value, dict):
                    # Dictionary of references
                    value = {key: frames.get(id) for key, id in value.items()}

                else:
                    value = frames.get(value, None)

                child_document = document
                keys = cls._path_to_keys(path)
                for key in keys[:-1]:
                    child_document = child_document[key]
                child_document[keys[-1]] = value

    @classmethod
    def _flatten_projection(cls, projection):
        """
        Flatten a structured projection (structure projections support for
        projections of (to be) dereferenced fields.
        """

        # If `projection` is empty return a full projection based on `_fields`
        if not projection:
            return {f: True for f in cls._fields}, {}, {}

        # Flatten the projection
        flat_projection = {}
        references = {}
        subs = {}
        inclusive = True
        for key, value in deepcopy(projection).items():
            if isinstance(value, dict):

                # Build the projection value for the field (allowing for
                # special mongo directives).
                project_value = {
                    k: v for k, v in value.items()
                    if k.startswith('$') and k not in ['$ref', '$sub', '$sub.']
                }
                if len(project_value) == 0:
                    project_value = True
                else:
                    project_value = {key: project_value}
                    inclusive = False

                # Store a reference/sub-frame projection
                if '$ref' in value:
                    references[key] = value

                elif '$sub' in value or '$sub.' in value:
                    subs[key] = value

                    if '$sub' in value:
                        sub_frame = value['$sub']

                    if '$sub.' in value:
                        sub_frame = value['$sub.']

                    project_value = sub_frame._projection_to_paths(key, value)

                if isinstance(project_value, dict):
                    flat_projection.update(project_value)

                else:
                    flat_projection[key] = project_value

            elif key == '$ref':
                # Strip any $ref key
                continue

            elif key == '$sub' or key == '$sub.':
                # Strip any $sub key
                continue

            elif key.startswith('$'):
                # Strip mongo operators
                continue

            else:
                # Store the root projection value
                flat_projection[key] = value
                inclusive = False

        # If only references and sub-frames where specified in the projection
        # then return a full projection based on `_fields`.
        if inclusive:
            flat_projection = {f: True for f in cls._fields}

        return flat_projection, references, subs

    # Integrity helpers

    @staticmethod
    def timestamp_insert(sender, frames):
        """
        Timestamp the created and modified fields for all documents. This method
        should be bound to a frame class like so:

        ```
        MyFrameClass.listen('insert', MyFrameClass.timestamp_insert)
        ```
        """
        for frame in frames:
            timestamp = datetime.now(timezone.utc)
            frame.created = timestamp
            frame.modified = timestamp

    @staticmethod
    def timestamp_update(sender, frames):
        """
        Timestamp the modified field for all documents. This method should be
        bound to a frame class like so:

        ```
        MyFrameClass.listen('update', MyFrameClass.timestamp_update)
        ```
        """
        for frame in frames:
            frame.modified = datetime.now(timezone.utc)

    # @classmethod
    # def cascade(cls, ref_cls, field, frames):
    #     """Apply a cascading delete (does not emit signals)"""
    #     ids = [to_refs(f[field]) for f in frames if f.get(field)]
    #     ref_cls.get_collection().delete_many({'_id': {'$in': ids}})

    @classmethod
    def nullify(cls, ref_cls, field, frames):
        """Nullify a reference field (does not emit signals)"""
        ids = [to_refs(f) for f in frames]
        ref_cls.get_collection().update_many(
            {field: {'$in': ids}},
            {'$set': {field: None}}
        )

    @classmethod
    def pull(cls, ref_cls, field, frames):
        """Pull references from a list field (does not emit signals)"""
        ids = [to_refs(f) for f in frames]
        ref_cls.get_collection().update_many(
            {field: {'$in': ids}},
            {'$pull': {field: {'$in': ids}}}
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

    # The documents defined fields
    # The documents defined fields
    _fields = set()

    # A set of private fields that will be excluded from the output of
    # `to_json_type`.
    _private_fields = set()

    @classmethod
    def _apply_projection(cls, documents, projection):

        # Find reference and sub-frame mappings
        references = {}
        subs = {}
        for key, value in deepcopy(projection).items():

            if not isinstance(value, dict):
                continue

            # Store a reference/sub-frame projection
            if '$ref' in value:
                references[key] = value
            elif '$sub' in value or '$sub.' in value:
                subs[key] = value

        # Dereference the documents (if required)
        if references:
            Frame._dereference(documents, references)

        # Add sub-frames to the documents (if required)
        if subs:
            Frame._apply_sub_frames(documents, subs)

    @classmethod
    def _projection_to_paths(cls, root_key, projection):
        """
        Expand a $sub/$sub. projection to a single projection of True (if
        inclusive) or a map of full paths (e.g `employee.company.tel`).
        """

        # Referenced projections are handled separately so just flag the
        # reference field to true.
        if '$ref' in projection:
            return True

        inclusive = True
        sub_projection = {}
        for key, value in projection.items():
            if key in ['$sub', '$sub.']:
                continue

            if key.startswith('$'):
                sub_projection[root_key] = {key: value}
                inclusive = False
                continue

            sub_key = root_key + '.' + key

            if isinstance(value, dict):
                sub_value = cls._projection_to_paths(sub_key, value)
                if isinstance(sub_value, dict):
                    sub_projection.update(sub_value)
                else:
                    sub_projection[sub_key] = True

            else:
                sub_projection[sub_key] = True
                inclusive = False

        if inclusive:
            # No specific keys so this is inclusive
            return True

        return sub_projection
