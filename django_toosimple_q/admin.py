from django.contrib import admin
from django.contrib.humanize.templatetags.humanize import naturaltime
from django.contrib.messages.constants import SUCCESS
from django.template.defaultfilters import truncatechars
from django.urls import reverse
from django.utils.html import format_html

from .models import Schedule, Task
from .registry import tasks


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    list_display = [
        "function",
        "args_",
        "kwargs_",
        "queue",
        "priority",
        "created_",
        "started_",
        "finished_",
        "icon",
        "result_",
    ]
    list_display_links = ["function"]
    list_filter = ["function", "queue", "state"]
    actions = ["action_requeue"]
    ordering = ["-created"]
    readonly_fields = ["args", "kwargs", "result"]

    def args_(self, obj):
        return truncatechars(str(obj.args), 32)

    def kwargs_(self, obj):
        return truncatechars(str(obj.kwargs), 32)

    def result_(self, obj):
        return truncatechars(str(obj.result), 32)

    def created_(self, obj):
        return naturaltime(obj.created)

    def started_(self, obj):
        return naturaltime(obj.started)

    def finished_(self, obj):
        return naturaltime(obj.finished)

    def action_requeue(self, request, queryset):
        for task in queryset:
            tasks[task.function].queue(*task.args, **task.kwargs)
        self.message_user(
            request, f"{queryset.count()} tasks successfully requeued...", level=SUCCESS
        )

    action_requeue.short_description = "Requeue task"


@admin.register(Schedule)
class ScheduleAdmin(admin.ModelAdmin):
    list_display = [
        "name",
        "function",
        "args",
        "kwargs",
        "cron",
        "last_check",
        "last_run_",
    ]
    list_display_links = ["name", "function"]
    ordering = ["last_check"]
    readonly_fields = ["args", "kwargs"]

    def last_run_(self, obj):
        if obj.last_run:
            app, model = obj.last_run._meta.app_label, obj.last_run._meta.model_name
            edit_link = reverse(f"admin:{app}_{model}_change", args=(obj.last_run_id,))
            return format_html('<a href="{}">{}</a>', edit_link, obj.last_run.icon)
        return "-"
