# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from django.urls import path
from django.contrib import admin


urlpatterns = [
    path("admin/", admin.site.urls),
]
