from django.contrib import admin
from django.contrib.humanize.templatetags.humanize import naturaltime
from django.contrib.messages.constants import SUCCESS
from django.template.defaultfilters import truncatechars
from django.urls import reverse
from django.utils.html import format_html

from .models import ScheduleExec, TaskExec
from .registry import tasks


@admin.register(TaskExec)
class TaskExecAdmin(admin.ModelAdmin):
    list_display = [
        "task_name",
        "args_",
        "kwargs_",
        "queue",
        "priority",
        "created_",
        "started_",
        "finished_",
        "icon",
        "replaced_by_",
        "result_",
    ]
    list_display_links = ["task_name"]
    list_filter = ["task_name", "queue", "state"]
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

    def replaced_by_(self, obj):
        if obj.replaced_by:
            return f"{obj.replaced_by.icon} [{obj.replaced_by.pk}]"

    def action_requeue(self, request, queryset):
        for task in queryset:
            tasks[task.task_name].queue(*task.args, **task.kwargs)
        self.message_user(
            request, f"{queryset.count()} tasks successfully requeued...", level=SUCCESS
        )

    action_requeue.short_description = "Requeue task"


@admin.register(ScheduleExec)
class ScheduleExecAdmin(admin.ModelAdmin):
    list_display = [
        "name",
        "last_check",
        "last_run_",
    ]
    list_display_links = ["name"]
    ordering = ["last_check"]

    def last_run_(self, obj):
        if obj.last_run:
            app, model = obj.last_run._meta.app_label, obj.last_run._meta.model_name
            edit_link = reverse(f"admin:{app}_{model}_change", args=(obj.last_run_id,))
            return format_html('<a href="{}">{}</a>', edit_link, obj.last_run.icon)
        return "-"
