"""LazyReference module.

This module contains functions, classes and mix-in that are used for the
building lazy references to objects located in database. These lazy references
will be evaluated only when some functions or properties will be called.

"""

import uuid
import time

import models
import lib.rome.driver.database_driver as database_driver
import traceback


def now_in_ms():
    return int(round(time.time() * 1000))


class EmptyObject:
    pass

class LazyAttribute(dict):
    """This class is used to intercept calls to emit_backref. This enables to have efficient lazy loading."""
    def __getitem__(self, item):
        return self

    def __getattr__(self, item):
        """This method 'intercepts' call to attribute/method."""
        if item in ["append"]:
            return self.append
        if item in ["pop"]:
            return self.pop
        if item in ["delete"]:
            return self.delete
        return self

    def append(self, *args, **kwargs):
        pass

    def pop(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

class LazyBackrefBuffer(object):
    """This class intercepts calls to emit_backref. This enables to have efficient lazy loading."""
    def __init__(self):
        self.attributes = []

    def __getattr__(self, item):
        """This method 'intercepts' call to attribute/method."""
        if item in ["manager", "parents"]:
            attribute = LazyAttribute()
            self.attributes += [attribute]
            return LazyAttribute()
        return getattr(self, item)


class LazyValue:
    """Class that represents a "Lazyfied" value. The LazyValue wraps a value in a dict format, and
    when an external object accesses one of the wrapped dict, content of the dict is "converted" in an object format
    (models entities). In a few words LazyValue(dict).id <-> JsonDeconverter(dict).id ."""

    def __init__(self, wrapped_dict, request_uuid):
        from lib.rome.core.dataformat import get_decoder
        self.deconverter = get_decoder(request_uuid=request_uuid)
        self.wrapped_dict = wrapped_dict
        self.wrapped_value = None
        self.request_uuid = request_uuid

    def transform(self, x):
        return self.deconverter.desimplify(x)

    def get_relationships(self):
        from utils import RelationshipModel
        result = []

        obj = self.wrapped_value.get_complex_ref()

        state = obj._sa_instance_state

        for field in obj._sa_class_manager:
            field_object = obj._sa_class_manager[field]
            field_column = state.mapper._props[field]

            contain_comparator = hasattr(field_object, "comparator")
            is_relationship = ("relationship" in str(field_object.comparator)
                               if contain_comparator else False
                               )
            if is_relationship:
                remote_local_pair = field_object.property.local_remote_pairs[0]

                local_fk_field = remote_local_pair[0].name
                local_fk_value = getattr(obj, local_fk_field)
                local_object_field = field
                local_object_value = getattr(obj, local_object_field)
                remote_object_field = remote_local_pair[1].name
                remote_object_tablename = str(remote_local_pair[1].table)
                is_list = field_object.property.uselist

                result += [RelationshipModel(
                    local_fk_field,
                    local_fk_value,
                    local_object_field,
                    local_object_value,
                    remote_object_field,
                    remote_object_tablename,
                    is_list
                )]

        return result

    def load_relationships(self, request_uuid=uuid.uuid1()):
        """Update foreign keys according to local fields' values."""
        from utils import LazyRelationshipList, LazyRelationshipSingleObject
        for rel in self.get_relationships():
            if rel.is_list:
                self.wrapped_value.get_complex_ref().__dict__[rel.local_object_field] = LazyRelationshipList(rel)
            else:
                self.wrapped_value.get_complex_ref().__dict__[rel.local_object_field] = LazyRelationshipSingleObject(rel)
        pass

    def __repr__(self):
        return "LazyValue(%s)" % (self.wrapped_dict)

    def get_key(self):
        """Returns a unique key for the current LazyReference."""
        return "%s_%s" % (self.resolve_model_name(), str(self.id))

    def resolve_model_name(self):
        """Returns the model class corresponding to the remote object."""
        if "metadata_novabase_classname" in self.wrapped_dict:
            return self.wrapped_dict["metadata_novabase_classname"]
        elif self.wrapped_value is not None:
            return models.get_model_classname_from_tablename(self.wrapped_value.base)
        else:
            return "None"

    def __getattr__(self, attr):
        if self.wrapped_value is None:
            self.wrapped_value = self.deconverter.desimplify(self.wrapped_dict)
        if "nova_classname" in self.wrapped_dict and "aggregate" in self.wrapped_dict["nova_classname"]:
            self.load_relationships()
        return getattr(self.wrapped_value, attr)


class LazyReference:
    """Class that references a remote object stored in database. This aims
    easing the development of algorithm on relational objects: instead of
    populating relationships even when not required, we load them "only" when
    it is used!"""

    def __init__(self, base, id, request_uuid, deconverter):
        """Constructor"""
        from lib.rome.core.dataformat import json as json_module
        caches = json_module.CACHES
        self.base = base
        self.id = id
        self.version = -1
        self.lazy_backref_buffer = LazyBackrefBuffer()
        self.request_uuid = request_uuid if request_uuid is not None else uuid.uuid1()
        if self.request_uuid not in caches:
            caches[self.request_uuid] = {}
        self.cache = caches[self.request_uuid]
        if deconverter is None:
            from lib.rome.core.dataformat import get_decoder
            self.deconverter = get_decoder(request_uuid=request_uuid)
        else:
            self.deconverter = deconverter
        self._session = None

    def set_session(self, session):
        self._session = session
        key = self.get_key()
        if key in self.cache:
            self.cache[key]._session = session

    def get_key(self):
        """Returns a unique key for the current LazyReference."""
        return "%s_%s" % (self.resolve_model_name(), str(self.id))

    def resolve_model_name(self):
        """Returns the model class corresponding to the remote object."""
        return models.get_model_classname_from_tablename(self.base)

    def spawn_empty_model(self, obj):
        """Spawn an empty instance of the model class specified by the
        given object"""
        key = self.get_key()
        if obj is not None:
            if"novabase_classname" in obj:
                model_class_name = obj["novabase_classname"]
            elif "metadata_novabase_classname" in obj:
                model_class_name = obj["metadata_novabase_classname"]
        else:
            model_class_name = self.resolve_model_name()
        if model_class_name is not None:
            model = models.get_model_class_from_name(model_class_name)
            model_object = model()
            if key not in self.cache:
                self.cache[key] = model_object
            return self.cache[key]
        else:
            return None

    def update_nova_model(self, obj):
        """Update the fields of the given object."""
        key = self.get_key()
        current_model = self.cache[key]

        if obj is None:
            return current_model

        # Check if obj is simplified or not
        if "simplify_strategy" in obj:
            obj = database_driver.get_driver().get(obj["tablename"], obj["id"])
        # For each value of obj, set the corresponding attributes.
        for key in obj:
            simplified_value = self.deconverter.desimplify(obj[key])
            try:
                if simplified_value is not None:
                    value = self.deconverter.desimplify(obj[key])
                    current_model[key] = value
                else:
                    current_model[key] = obj[key]
            except Exception as e:
                if "None is not list-like" in str(e):
                    setattr(current_model, key, [])
                else:
                    traceback.print_exc()
                    pass
        if hasattr(current_model, "user_id") and obj.has_key("user_id"):
            current_model.user_id = obj["user_id"]
        if hasattr(current_model, "project_id") and obj.has_key("project_id"):
            current_model.project_id = obj["project_id"]
        return current_model

    def load(self, data=None):
        """Load the referenced object from the database. The result will be
        cached, so that next call will not create any database request."""
        self.version = 0
        key = self.get_key()
        first_load = data is None
        if first_load:
            data = database_driver.get_driver().get(self.base, self.id)
        self.spawn_empty_model(data)
        self.update_nova_model(data)
        # if first_load and "aggregate" in self.base:
        if first_load:
            self.load_relationships()
            self.update_nova_model(data)
        if self._session is not None:
            self.cache[key]._session = self._session
        return self.cache[key]

    def get_complex_ref(self):
        """Return the python object that corresponds the referenced object. The
        first time this method has been invocked, a request to the database is
        made and the result is cached. The next times this method is invocked,
        the previously cached result is returned."""
        key = self.get_key()
        if not key in self.cache:
            self.load()
        return self.cache[key]

    def __getattr__(self, item):
        """This method 'intercepts' call to attribute/method on the referenced
        object: the object is thus loaded from database, and the requested
        attribute/method is then returned."""
        if item == "_sa_instance_state":
            key = self.get_key()
            if not self.cache.has_key(key):
                return self.lazy_backref_buffer
        return getattr(self.get_complex_ref(), item)

    def __setattr__(self, name, value):
        """This method 'intercepts' affectation to attribute/method on the
        referenced object: the object is thus loaded from database, and the
        requested attribute/method is then setted with the given value."""
        if name in ["base", "id", "cache", "deconverter", "request_uuid",
                    "uuid", "version", "lazy_backref_buffer", "_session", "_version"]:
            self.__dict__[name] = value
        else:
            setattr(self.get_complex_ref(), name, value)
            if self._session is not None:
                self._session.add(self)
            return self

    def __str__(self):
        """This method prevents the loading of the remote object when a
        LazyReference is printed."""
        return "Lazy(%s:%s:%d)" % (self.get_key(), self.base, self.version)

    def __repr__(self):
        """This method prevents the loading of the remote object when a
        LazyReference is printed."""
        return "Lazy(%s:%s:%d)" % (self.get_key(), self.base, self.version)

    def __hash__(self):
        """This method prevents the loading of the remote object when a
        LazyReference is stored in a dict."""
        return self.__str__().__hash__()

    def __nonzero__(self):
        """This method is required by some services of OpenStack."""
        return not not self.get_complex_ref()
