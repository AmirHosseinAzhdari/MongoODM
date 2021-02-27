from django.conf.urls import url
from django.contrib import admin
from django.urls import path

from user.views import User

urlpatterns = [
    url('1/', User.as_view()),
]
