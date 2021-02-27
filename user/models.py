# Create your models here.
from base.db.frames_motor.frames import Frame
from base.db.fields import *


# Frame._client = AsyncIOMotorClient(
#     "mongodb://root:example@10.10.10.20:27018/test1?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false")

#
class user(Frame):
    password = CharField(max_length=100, null=True)
    first_name = CharField(max_length=70, null=True)
    last_name = CharField(max_length=100, null=True)
    national_code = CharField(max_length=10, null=True)
    is_foreign_national = BooleanField(default=True)
    foreign_national_image = CharField(max_length=200, null=True)
    mobile_number = CharField(max_length=10)  # Fix Me
#
