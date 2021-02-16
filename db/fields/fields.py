import ast
import datetime
import uuid
import json
from io import BytesIO
from bson import ObjectId
from django.core import checks, exceptions, validators
from django.core.validators import FileExtensionValidator, get_available_image_extensions
from django.db.models.query_utils import DeferredAttribute
from django.utils import timezone
from django.utils.dateparse import (
    parse_date, parse_datetime, parse_time,
)
from django.utils.functional import cached_property
from django.utils.ipv6 import clean_ipv6_address
from django.utils.translation import gettext_lazy as _, ngettext_lazy
from django.forms.widgets import (
    FILE_INPUT_CONTRADICTION, CheckboxInput, ClearableFileInput, DateInput,
    DateTimeInput, EmailInput, FileInput, HiddenInput, MultipleHiddenInput,
    NullBooleanSelect, NumberInput, Select, SelectMultiple,
    SplitDateTimeWidget, SplitHiddenDateTimeWidget, Textarea, TextInput,
    TimeInput, URLInput
)
import magic

# from db.frames.frames import _BaseFrame

__all__ = [
    'BooleanField', 'CharField',
    'DateField', 'DateTimeField',
    'EmailField', 'Field', 'FloatField', 'GenericIPAddressField',
    'IPAddressField', 'IntegerField', 'NOT_PROVIDED', 'SlugField',
    'TextField', 'TimeField', 'URLField', 'UUIDField', 'ArrayField', 'JSONField', 'ForeignFrame', 'ForeignKey',
    'ObjectIdField',
    'EmbeddedField'
]


class Empty:
    pass


class NOT_PROVIDED:
    pass


def _empty(of_cls):
    new = Empty()
    new.__class__ = of_cls
    return new


def return_None():
    return None


class Field:  # RegisterLookupMixin
    """Base class for all field types"""

    # Designates whether empty strings fundamentally are allowed at the
    # database level.
    empty_strings_allowed = True
    empty_values = list(validators.EMPTY_VALUES)

    # These track each time a Field instance is created. Used to retain order.
    # The auto_creation_counter is used for fields that Django implicitly
    # creates, creation_counter is used for all user-specified fields.
    creation_counter = 0
    auto_creation_counter = -1
    default_validators = []  # Default set of validators
    default_error_messages = {
        'invalid_choice': _('Value %(value)r is not a valid choice.'),
        'null': _('This field cannot be null.'),
        'blank': _('This field cannot be blank.'),
        'unique': _('%(model_name)s with this %(field_label)s '
                    'already exists.'),
        # Translators: The 'lookup_type' is one of 'date', 'year' or 'month'.
        # Eg: "Title must be unique for pub_date year"
        'unique_for_date': _("%(field_label)s must be unique for "
                             "%(date_field_label)s %(lookup_type)s."),
    }
    system_check_deprecated_details = None
    system_check_removed_details = None

    # Field flags
    hidden = False

    many_to_many = None
    many_to_one = None
    one_to_many = None
    one_to_one = None
    related_model = None

    descriptor_class = DeferredAttribute

    # Generic field type description, usually overridden by subclasses
    def _description(self):
        return _('Field of type: %(field_type)s') % {
            'field_type': self.__class__.__name__
        }

    description = property(_description)

    def __init__(self, max_length=None, null=False,
                 default=NOT_PROVIDED, validators=(),
                 error_messages=None):
        self.max_length = max_length
        self.null = null
        self.default = default
        # Adjust the appropriate creation counter, and save our local copy.
        self._validators = list(validators)  # Store for deconstruction later
        messages = {}
        for c in reversed(self.__class__.__mro__):
            messages.update(getattr(c, 'default_error_messages', {}))
        messages.update(error_messages or {})
        self._error_messages = error_messages  # Store for deconstruction later
        self.error_messages = messages

    def __repr__(self):
        """Display the module, class, and name of the field."""
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__qualname__)
        name = getattr(self, 'name', None)
        if name is not None:
            return '<%s: %s>' % (path, name)
        return '<%s>' % path

    def _check_validators(self):
        errors = []
        for i, validator in enumerate(self.validators):
            if not callable(validator):
                errors.append(
                    checks.Error(
                        "All 'validators' must be callable.",
                        hint=(
                            "validators[{i}] ({repr}) isn't a function or "
                            "instance of a validator class.".format(
                                i=i, repr=repr(validator),
                            )
                        ),
                        obj=self,
                        id='fields.E008',
                    )
                )
        return errors

    def to_python(self, value):
        """
        Convert the input value into the expected Python data type, raising
        django.core.exceptions.ValidationError if the data can't be converted.
        Return the converted value. Subclasses should override this.
        """
        return value

    @cached_property
    def validators(self):
        """
        Some validators can't be created at field initialization time.
        This method provides a way to delay their creation until required.
        """
        return [*self.default_validators, *self._validators]

    def run_validators(self, value):
        if value in self.empty_values:
            return

        errors = []
        for v in self.validators:
            try:
                v(value)
            except exceptions.ValidationError as e:
                if hasattr(e, 'code') and e.code in self.error_messages:
                    e.message = self.error_messages[e.code]
                errors.extend(e.error_list)

        if errors:
            raise exceptions.ValidationError(errors)

    def validate(self, value):
        if value not in self.empty_values:
            return value

        if value is None and not self.null:
            raise exceptions.ValidationError(self.error_messages['null'], code='null')

        if value not in self.empty_values:
            raise exceptions.ValidationError(self.error_messages['blank'], code='blank')

    def clean(self, value):
        """
        Convert the value's type and run validation. Validation errors
        from to_python() and validate() are propagated. Return the correct
        value if no error is raised.
        """
        value = self.to_python(value)
        self.validate(value)
        self.run_validators(value)
        return value

    def has_default(self):
        """Return a boolean of whether this field has a default value."""
        return self.default is not NOT_PROVIDED

    def get_default(self):
        """Return the default value for this field."""
        return self._get_default()

    def widget_attrs(self, widget):
        """
        Given a Widget instance (*not* a Widget class), return a dictionary of
        any HTML attributes that should be added to the Widget, based on this
        Field.
        """
        return {}

    @cached_property
    def _get_default(self):
        if self.has_default():
            if callable(self.default):
                return self.default
            return lambda: self.default

        if not self.empty_strings_allowed or self.null:
            return return_None
        return str  # return empty string


class BooleanField(Field):
    empty_strings_allowed = False
    default_error_messages = {
        'invalid': _('“%(value)s” value must be either True or False.'),
        'invalid_nullable': _('“%(value)s” value must be either True, False, or None.'),
    }
    description = _("Boolean (Either True or False)")

    def get_internal_type(self):
        return "BooleanField"

    def to_python(self, value):
        if self.null and value in self.empty_values:
            return None
        if value in (True, False):
            # 1/0 are equal to True/False. bool() converts former to latter.
            return bool(value)
        if value in ('t', 'True', '1'):
            return True
        if value in ('f', 'False', '0'):
            return False
        raise exceptions.ValidationError(
            self.error_messages['invalid_nullable' if self.null else 'invalid'],
            code='invalid',
            params={'value': value},
        )


class CharField(Field):
    description = _("String (up to %(max_length)s)")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.validators.append(validators.MaxLengthValidator(self.max_length))

    def check(self, **kwargs):
        return [
            # *super().check(**kwargs),
            *self._check_max_length_attribute(**kwargs),
        ]

    def _check_max_length_attribute(self, **kwargs):
        if self.max_length is None:
            return [
                checks.Error(
                    "CharFields must define a 'max_length' attribute.",
                    obj=self,
                    id='fields.E120',
                )
            ]
        elif (not isinstance(self.max_length, int) or isinstance(self.max_length, bool) or
              self.max_length <= 0):
            return [
                checks.Error(
                    "'max_length' must be a positive integer.",
                    obj=self,
                    id='fields.E121',
                )
            ]
        else:
            return []

    def get_internal_type(self):
        return "CharField"

    def to_python(self, value):
        if isinstance(value, str) or value is None:
            return value
        return str(value)


class DateTimeCheckMixin:

    def check(self, **kwargs):
        return [
            *self._check_mutually_exclusive_options(),
            *self._check_fix_default_value(),
        ]

    def _check_mutually_exclusive_options(self):
        # auto_now, auto_now_add, and default are mutually exclusive
        # options. The use of more than one of these options together
        # will trigger an Error
        mutually_exclusive_options = []
        enabled_options = [option not in (None, False) for option in mutually_exclusive_options].count(True)
        if enabled_options > 1:
            return [
                checks.Error(
                    "The options auto_now, auto_now_add, and default "
                    "are mutually exclusive. Only one of these options "
                    "may be present.",
                    obj=self,
                    id='fields.E160',
                )
            ]
        else:
            return []

    def _check_fix_default_value(self):
        return []


class DateField(Field):
    empty_strings_allowed = False
    default_error_messages = {
        'invalid': _('“%(value)s” value has an invalid date format. It must be '
                     'in YYYY-MM-DD format.'),
        'invalid_date': _('“%(value)s” value has the correct format (YYYY-MM-DD) '
                          'but it is an invalid date.'),
    }
    description = _("Date (without time)")

    def _check_fix_default_value(self):
        """
        Warn that using an actual date or datetime value is probably wrong;
        it's only evaluated on server startup.
        """
        if not self.has_default():
            return []

        now = timezone.now()
        if not timezone.is_naive(now):
            now = timezone.make_naive(now, timezone.utc)
        value = self.default
        if isinstance(value, datetime.datetime):
            if not timezone.is_naive(value):
                value = timezone.make_naive(value, timezone.utc)
            value = value.date()
        elif isinstance(value, datetime.date):
            # Nothing to do, as dates don't have tz information
            pass
        else:
            # No explicit date / datetime value -- no checks necessary
            return []
        offset = datetime.timedelta(days=1)
        lower = (now - offset).date()
        upper = (now + offset).date()
        if lower <= value <= upper:
            return [
                checks.Warning(
                    'Fixed default value provided.',
                    hint='It seems you set a fixed date / time / datetime '
                         'value as default for this field. This may not be '
                         'what you want. If you want to have the current date '
                         'as default, use `django.utils.timezone.now`',
                    obj=self,
                    id='fields.W161',
                )
            ]

        return []

    def get_internal_type(self):
        return "DateField"

    def to_python(self, value):
        if value is None:
            return value
        if isinstance(value, datetime.datetime):
            # if settings.USE_TZ and timezone.is_aware(value):
            #     # Convert aware datetimes to the default time zone
            #     # before casting them to dates (#17742).
            #     default_timezone = timezone.get_default_timezone()
            #     value = timezone.make_naive(value, default_timezone)
            return value.date()
        if isinstance(value, datetime.date):
            return value

        try:
            parsed = parse_date(value)
            if parsed is not None:
                return parsed
        except ValueError:
            raise exceptions.ValidationError(
                self.error_messages['invalid_date'],
                code='invalid_date',
                params={'value': value},
            )

        raise exceptions.ValidationError(
            self.error_messages['invalid'],
            code='invalid',
            params={'value': value},
        )


class DateTimeField(DateField):
    empty_strings_allowed = False
    default_error_messages = {
        'invalid': _('“%(value)s” value has an invalid format. It must be in '
                     'YYYY-MM-DD HH:MM[:ss[.uuuuuu]][TZ] format.'),
        'invalid_date': _("“%(value)s” value has the correct format "
                          "(YYYY-MM-DD) but it is an invalid date."),
        'invalid_datetime': _('“%(value)s” value has the correct format '
                              '(YYYY-MM-DD HH:MM[:ss[.uuuuuu]][TZ]) '
                              'but it is an invalid date/time.'),
    }
    description = _("Date (with time)")

    # __init__ is inherited from DateField

    def _check_fix_default_value(self):
        """
        Warn that using an actual date or datetime value is probably wrong;
        it's only evaluated on server startup.
        """
        if not self.has_default():
            return []

        now = timezone.now()
        if not timezone.is_naive(now):
            now = timezone.make_naive(now, timezone.utc)
        value = self.default
        if isinstance(value, datetime.datetime):
            second_offset = datetime.timedelta(seconds=10)
            lower = now - second_offset
            upper = now + second_offset
            if timezone.is_aware(value):
                value = timezone.make_naive(value, timezone.utc)
        elif isinstance(value, datetime.date):
            second_offset = datetime.timedelta(seconds=10)
            lower = now - second_offset
            lower = datetime.datetime(lower.year, lower.month, lower.day)
            upper = now + second_offset
            upper = datetime.datetime(upper.year, upper.month, upper.day)
            value = datetime.datetime(value.year, value.month, value.day)
        else:
            # No explicit date / datetime value -- no checks necessary
            return []
        if lower <= value <= upper:
            return [
                checks.Warning(
                    'Fixed default value provided.',
                    hint='It seems you set a fixed date / time / datetime '
                         'value as default for this field. This may not be '
                         'what you want. If you want to have the current date '
                         'as default, use `django.utils.timezone.now`',
                    obj=self,
                    id='fields.W161',
                )
            ]

        return []

    def get_internal_type(self):
        return "DateTimeField"

    def to_python(self, value):
        if value is None:
            return value
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            value = datetime.datetime(value.year, value.month, value.day)
            return value

        try:
            parsed = parse_datetime(value)
            if parsed is not None:
                return parsed
        except ValueError:
            raise exceptions.ValidationError(
                self.error_messages['invalid_datetime'],
                code='invalid_datetime',
                params={'value': value},
            )

        try:
            parsed = parse_date(value)
            if parsed is not None:
                return datetime.datetime(parsed.year, parsed.month, parsed.day)
        except ValueError:
            raise exceptions.ValidationError(
                self.error_messages['invalid_date'],
                code='invalid_date',
                params={'value': value},
            )

        raise exceptions.ValidationError(
            self.error_messages['invalid'],
            code='invalid',
            params={'value': value},
        )


class EmailField(CharField):
    default_validators = [validators.validate_email]
    description = _("Email address")

    def __init__(self, *args, **kwargs):
        # max_length=254 to be compliant with RFCs 3696 and 5321
        kwargs.setdefault('max_length', 254)
        super().__init__(*args, **kwargs)


class FloatField(Field):
    empty_strings_allowed = False
    default_error_messages = {
        'invalid': _('“%(value)s” value must be a float.'),
    }
    description = _("Floating point number")

    def to_python(self, value):
        if value is None:
            return value
        try:
            return float(value)
        except (TypeError, ValueError):
            raise exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )


class IntegerField(Field):
    empty_strings_allowed = False
    default_error_messages = {
        'invalid': _('“%(value)s” value must be an integer.'),
    }
    description = _("Integer")

    def check(self, **kwargs):
        return [
            *self._check_max_length_warning(),
        ]

    def _check_max_length_warning(self):
        if self.max_length is not None:
            return [
                checks.Warning(
                    "'max_length' is ignored when used with %s." % self.__class__.__name__,
                    hint="Remove 'max_length' from field",
                    obj=self,
                    id='fields.W122',
                )
            ]
        return []

    @cached_property
    def validators(self):
        # These validators can't be added at field initialization time since
        # they're based on values retrieved from `connection`.
        validators_ = super().validators
        internal_type = self.get_internal_type()
        return validators_

    def get_internal_type(self):
        return "IntegerField"

    def to_python(self, value):
        if value is None:
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            raise exceptions.ValidationError(
                self.error_messages['invalid'],
                code='invalid',
                params={'value': value},
            )


class IPAddressField(Field):
    empty_strings_allowed = False
    description = _("IPv4 address")
    system_check_removed_details = {
        'msg': (
            'IPAddressField has been removed except for support in '
            'historical migrations.'
        ),
        'hint': 'Use GenericIPAddressField instead.',
        'id': 'fields.E900',
    }

    def __init__(self, *args, **kwargs):
        kwargs['max_length'] = 15
        super().__init__(*args, **kwargs)

    def get_internal_type(self):
        return "IPAddressField"


class GenericIPAddressField(Field):
    empty_strings_allowed = False
    description = _("IP address")
    default_error_messages = {}

    def __init__(self, verbose_name=None, name=None, protocol='both',
                 unpack_ipv4=False, *args, **kwargs):
        self.unpack_ipv4 = unpack_ipv4
        self.protocol = protocol
        self.default_validators, invalid_error_message = \
            validators.ip_address_validators(protocol, unpack_ipv4)
        self.default_error_messages['invalid'] = invalid_error_message
        kwargs['max_length'] = 39
        super().__init__(verbose_name, name, *args, **kwargs)

    def check(self, **kwargs):
        return [
            *self._check_blank_and_null_values(**kwargs),
        ]

    def _check_blank_and_null_values(self, **kwargs):
        if not getattr(self, 'null', False) and getattr(self, 'blank', False):
            return [
                checks.Error(
                    'GenericIPAddressFields cannot have blank=True if null=False, '
                    'as blank values are stored as nulls.',
                    obj=self,
                    id='fields.E150',
                )
            ]
        return []

    def get_internal_type(self):
        return "GenericIPAddressField"

    def to_python(self, value):
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        value = value.strip()
        if ':' in value:
            return clean_ipv6_address(value, self.unpack_ipv4, self.error_messages['invalid'])
        return value


class SlugField(CharField):
    default_validators = [validators.validate_slug]
    description = _("Slug (up to %(max_length)s)")

    def __init__(self, *args, max_length=50, db_index=True, allow_unicode=False, **kwargs):
        self.allow_unicode = allow_unicode
        if self.allow_unicode:
            self.default_validators = [validators.validate_unicode_slug]
        super().__init__(*args, max_length=max_length, db_index=db_index, **kwargs)

    def get_internal_type(self):
        return "SlugField"


class TextField(Field):
    description = _("Text")

    def get_internal_type(self):
        return "TextField"

    def to_python(self, value):
        if isinstance(value, str) or value is None:
            return value
        return str(value)


class TimeField(DateTimeCheckMixin, Field):
    empty_strings_allowed = False
    default_error_messages = {
        'invalid': _('“%(value)s” value has an invalid format. It must be in '
                     'HH:MM[:ss[.uuuuuu]] format.'),
        'invalid_time': _('“%(value)s” value has the correct format '
                          '(HH:MM[:ss[.uuuuuu]]) but it is an invalid time.'),
    }
    description = _("Time")

    def _check_fix_default_value(self):
        """
        Warn that using an actual date or datetime value is probably wrong;
        it's only evaluated on server startup.
        """
        if not self.has_default():
            return []

        now = timezone.now()
        if not timezone.is_naive(now):
            now = timezone.make_naive(now, timezone.utc)
        value = self.default
        if isinstance(value, datetime.datetime):
            second_offset = datetime.timedelta(seconds=10)
            lower = now - second_offset
            upper = now + second_offset
            if timezone.is_aware(value):
                value = timezone.make_naive(value, timezone.utc)
        elif isinstance(value, datetime.time):
            second_offset = datetime.timedelta(seconds=10)
            lower = now - second_offset
            upper = now + second_offset
            value = datetime.datetime.combine(now.date(), value)
            if timezone.is_aware(value):
                value = timezone.make_naive(value, timezone.utc).time()
        else:
            # No explicit time / datetime value -- no checks necessary
            return []
        if lower <= value <= upper:
            return [
                checks.Warning(
                    'Fixed default value provided.',
                    hint='It seems you set a fixed date / time / datetime '
                         'value as default for this field. This may not be '
                         'what you want. If you want to have the current date '
                         'as default, use `django.utils.timezone.now`',
                    obj=self,
                    id='fields.W161',
                )
            ]

        return []

    def get_internal_type(self):
        return "TimeField"

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.time):
            return value
        if isinstance(value, datetime.datetime):
            # Not usually a good idea to pass in a datetime here (it loses
            # information), but this can be a side-effect of interacting with a
            # database backend (e.g. Oracle), so we'll be accommodating.
            return value.time()

        try:
            parsed = parse_time(value)
            if parsed is not None:
                return parsed
        except ValueError:
            raise exceptions.ValidationError(
                self.error_messages['invalid_time'],
                code='invalid_time',
                params={'value': value},
            )

        raise exceptions.ValidationError(
            self.error_messages['invalid'],
            code='invalid',
            params={'value': value},
        )


class URLField(CharField):
    default_validators = [validators.URLValidator()]
    description = _("URL")

    def __init__(self, verbose_name=None, name=None, **kwargs):
        kwargs.setdefault('max_length', 200)
        super().__init__(verbose_name, name, **kwargs)


class UUIDField(Field):
    default_error_messages = {
        'invalid': _('“%(value)s” is not a valid UUID.'),
    }
    description = _('Universally unique identifier')
    empty_strings_allowed = False

    def __init__(self, verbose_name=None, **kwargs):
        kwargs['max_length'] = 32
        super().__init__(verbose_name, **kwargs)

    def get_internal_type(self):
        return "UUIDField"

    def to_python(self, value):
        if value is not None and not isinstance(value, uuid.UUID):
            input_form = 'int' if isinstance(value, int) else 'hex'
            try:
                return uuid.UUID(**{input_form: value})
            except (AttributeError, ValueError):
                raise exceptions.ValidationError(
                    self.error_messages['invalid'],
                    code='invalid',
                    params={'value': value},
                )
        return value


class FileField(Field):
    allowed_extentions = []

    def __init__(self, allowed_extentions=None, Image=True, **kwargs):
        if allowed_extentions is not None:
            self.allowed_extentions = allowed_extentions
        elif Image:
            from PIL import Image
            for key, value in Image.EXTENSION.items():
                self.allowed_extentions.append(key.lower())
                self.allowed_extentions.append(value.lower())
        if allowed_extentions is None:
            self.allowed_extentions = True

        super().__init__(**kwargs)

    def clean(self, data=None):
        if self.null is False:
            if data is None:
                raise exceptions.ValidationError(
                    self.error_messages['file is required'],
                    code='required',
                )
        if data is None:
            return data
        mime = magic.Magic(mime=True)
        file_extention = mime.from_buffer(data.read()).split("/")[1]
        if self.allowed_extentions is True:
            return data
        if file_extention in self.allowed_extentions:
            return self.check_format(data)
        else:
            raise exceptions.ValidationError(
                self.error_messages['invalid extention'],
                code='invalid_extentions',
            )
        return self.check_format(data)

    def check_format(self, data):
        if data.name.split(".")[1].lower() in self.allowed_extentions:
            return data
        raise exceptions.ValidationError(
            self.error_messages['invalid extention'],
            code='invalid_extentions',
        )


class JSONField(Field):

    def to_python(self, value):
        try:
            return json.loads(value)
        except:
            raise exceptions.ValidationError(
                self.error_messages['invalid json format'],
                code='invalid_json_format',
            )


class ArrayField(Field):
    def __init__(self, to: Field = None, default=list()):
        super(ArrayField, self).__init__(default=default)
        self.to = to

    def clean(self, value):
        try:
            value = json.loads(value)
        except:
            raise exceptions.ValidationError(
                self.error_messages['invalid list format'],
                code='invalid_list_format',
            )
        # if isinstance(self.to, _BaseFrame):
        #     for idx, item in enumerate(value):
        #         value[idx] = self.to.is_valid(item)
        if isinstance(self.to, Field):
            for idx, item in enumerate(value):
                value[idx] = self.to.clean(item)
        return value


# ---------------------------------------------------------------------------------------------------

# ------------------------foreignkey and foreign frame need to be modify.... ------------------------

# ---------------------------------------------------------------------------------------------------
class ForeignKey(Field):
    def __init__(self, redis=None, collection=None, frame=None, service=None, on_delete=None
                 ):
        super(ForeignKey, self).__init__()
        self.on_delete = on_delete
        self.redis = redis
        self.collection = collection
        self.frame = frame
        self.service = service

    def to_python(self, value):
        try:
            value = ObjectId(value)
        except:
            raise exceptions.ValidationError(
                self.error_messages['invalid object id'],
                code='invalid_object_id',
            )


class EmbeddedField(Field):
    def __init__(self, to, default=None, null=True):
        self.null = null
        super(EmbeddedField, self).__init__(default=default, null=null)
        self.to = to

    def to_python(self, value):
        if value:
            if not isinstance(value, self.to):
                value = self.to(value)
        return value

    def clean(self, value):
        value = super(EmbeddedField, self).clean(value)
        if value:
            value.is_valid()
        return value


class ObjectIdField(Field):
    def __init__(self, *args, **kwargs):
        super(ObjectIdField, self).__init__(max_length=24, *args, **kwargs)

    def to_python(self, value):
        if self.null and value in self.empty_values:
            return None
        if value and not isinstance(value, ObjectId):
            return ObjectId(value)
        if self.null:
            return value
        return ObjectId()

    def clean(self, value):
        super(ObjectIdField, self).clean(value)
        if value is not None:
            if len(str(value)) is not 24:
                raise exceptions.ValidationError(
                    self.error_messages['invalid object id'],
                    code='invalid_object_id',
                )
        return value


class ForeignFrame:
    def __init__(self, redis=False, frame=None, queue=False, on_delete=False):
        self.on_delete = on_delete
        self.redis = redis
        self.frame = frame
        self.queue = queue
