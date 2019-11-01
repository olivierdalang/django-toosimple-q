from django.contrib import admin
from django.contrib.humanize.templatetags.humanize import naturaltime
from django.urls import reverse
from django.utils.html import format_html

from .models import Task, Schedule


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = ['function', 'args', 'kwargs', 'created_', 'started_', 'finished_', 'icon', 'result']
    list_display_links = ['function']
    ordering = ['-created']
    readonly_fields = ['args', 'kwargs', 'result']

    def created_(self, obj):
        return naturaltime(obj.created)

    def started_(self, obj):
        return naturaltime(obj.started)

    def finished_(self, obj):
        return naturaltime(obj.finished)


@admin.register(Schedule)
class ScheduleAdmin(admin.ModelAdmin):
    list_display = ['name', 'function', 'args', 'kwargs', 'cron', 'last_check', 'last_run_']
    list_display_links = ['name', 'function']
    ordering = ['last_check']
    readonly_fields = ['args', 'kwargs']

    def last_run_(self, obj):
        if obj.last_run:
            app, model = obj.last_run._meta.app_label, obj.last_run._meta.model_name
            edit_link = reverse(f"admin:{app}_{model}_change", args=(obj.last_run_id,))
            return format_html('<a href="{}">{}</a>', edit_link, obj.last_run.icon)
        return "-"
