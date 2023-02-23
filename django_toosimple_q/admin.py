from datetime import datetime

from croniter import croniter
from django.contrib import admin
from django.contrib.messages.constants import SUCCESS
from django.template.defaultfilters import truncatechars
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone
from django.utils.formats import date_format
from django.utils.html import escape, format_html
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from .models import ScheduleExec, TaskExec, WorkerStatus
from .registry import schedules_registry, tasks_registry


class AbstractQueueListFilter(admin.SimpleListFilter):
    title = _("queue")
    parameter_name = "queue"
    registry = None
    name_field = None

    def lookups(self, request, model_admin):
        queues = set(item.queue for item in self.registry.values())
        return [(q, q) for q in sorted(list(queues))]

    def queryset(self, request, queryset):
        queue = self.value()
        if queue:
            names = [
                item.name for item in self.registry.values() if item.queue == queue
            ]
            return queryset.filter(**{f"{self.name_field}__in": names})


class TaskQueueListFilter(AbstractQueueListFilter):
    registry = tasks_registry
    name_field = "task_name"


class ScheduleQueueListFilter(AbstractQueueListFilter):
    registry = schedules_registry
    name_field = "name"


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_change_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False


@admin.register(TaskExec)
class TaskExecAdmin(ReadOnlyAdmin):
    list_display = [
        "icon",
        "task_name",
        "arguments_",
        "due_",
        "created_",
        "started_",
        "finished_",
        "replaced_by_",
        "result_",
        "task_",
    ]
    list_display_links = ["task_name"]
    list_filter = ["task_name", TaskQueueListFilter, "state"]
    actions = ["action_requeue"]
    ordering = ["-created"]
    readonly_fields = ["task_", "result"]

    def arguments_(self, obj):
        return format_html(
            "{}<br/>{}",
            truncatechars(str(obj.args), 32),
            truncatechars(str(obj.kwargs), 32),
        )

    def result_(self, obj):
        return truncatechars(str(obj.result), 32)

    @admin.display(ordering="due")
    def due_(self, obj):
        return short_naturaltime(obj.due)

    @admin.display(ordering="created")
    def created_(self, obj):
        return short_naturaltime(obj.created)

    @admin.display(ordering="started")
    def started_(self, obj):
        return short_naturaltime(obj.started)

    @admin.display(ordering="finished")
    def finished_(self, obj):
        return short_naturaltime(obj.finished)

    def replaced_by_(self, obj):
        if obj.replaced_by:
            return f"{obj.replaced_by.icon} [{obj.replaced_by.pk}]"

    def task_(self, obj):
        if not obj.task:
            return None
        return render_to_string("toosimpleq/task.html", {"task": obj.task})

    @admin.display(description="Requeue task")
    def action_requeue(self, request, queryset):
        for task in queryset:
            tasks_registry[task.task_name].enqueue(*task.args, **task.kwargs)
        self.message_user(
            request, f"{queryset.count()} tasks successfully requeued", level=SUCCESS
        )


@admin.register(ScheduleExec)
class ScheduleExecAdmin(ReadOnlyAdmin):
    list_display = [
        "icon",
        "name",
        "last_due_",
        "next_due_",
        "last_task_",
        "schedule_",
    ]
    list_display_links = ["name"]
    ordering = ["-last_due"]
    list_filter = ["name", ScheduleQueueListFilter, "state"]
    actions = ["action_force_run"]
    readonly_fields = ["schedule_"]

    def schedule_(self, obj):
        if not obj.schedule:
            return None
        return render_to_string("toosimpleq/schedule.html", {"schedule": obj.schedule})

    def last_task_(self, obj):
        if obj.last_task:
            app, model = obj.last_task._meta.app_label, obj.last_task._meta.model_name
            edit_link = reverse(f"admin:{app}_{model}_change", args=(obj.last_task_id,))
            return format_html('<a href="{}">{}</a>', edit_link, obj.last_task)
        return "-"

    @admin.display(ordering="last_due")
    def last_due_(self, obj):
        return short_naturaltime(obj.last_due)

    @admin.display()
    def next_due_(self, obj):
        if obj.next_dues:
            next_due = obj.next_dues[0]
        else:
            next_due = croniter(obj.schedule.cron, timezone.now()).get_next(datetime)

        formatted_next_due = short_naturaltime(next_due)
        if len(obj.next_dues) > 1:
            formatted_next_due += mark_safe(f" [×{len(obj.next_dues)}]")
        if next_due < timezone.now():
            return mark_safe(f"<span style='color: red'>{formatted_next_due}</span>")
        return formatted_next_due

    @admin.display(description="Force run schedule")
    def action_force_run(self, request, queryset):
        for schedule_exec in queryset:
            schedule_exec.schedule.execute(force=True)
        self.message_user(
            request,
            f"{queryset.count()} schedules successfully executed",
            level=SUCCESS,
        )


@admin.register(WorkerStatus)
class WorkerStatusAdmin(ReadOnlyAdmin):
    list_display = [
        "icon",
        "label",
        "last_tick_",
        "started_",
        "stopped_",
        "included_queues",
        "excluded_queues",
    ]
    list_display_links = ["label"]
    ordering = ["-started", "label"]
    readonly_fields = ["state"]

    @admin.display(ordering="last_tick")
    def last_tick_(self, obj):
        return short_naturaltime(obj.last_tick)

    @admin.display(ordering="started")
    def started_(self, obj):
        return short_naturaltime(obj.started)

    @admin.display(ordering="stopped")
    def stopped_(self, obj):
        return short_naturaltime(obj.stopped)


def short_naturaltime(datetime):
    if datetime is None:
        return None

    disps = [
        (60, "s"),
        (60 * 60, "m"),
        (60 * 60 * 24, "h"),
        (60 * 60 * 24 * 7, "D"),
        (60 * 60 * 24 * 30, "W"),
        (60 * 60 * 24 * 365, "M"),
        (float("inf"), "Y"),
    ]

    delta = timezone.now() - datetime
    seconds = delta.total_seconds()

    last_v = 1
    for threshold, abbr in disps:
        if abs(seconds) < threshold:
            text = f"{int(abs(seconds) // last_v)}{abbr}"
            break
        last_v = threshold

    shorttime = f"in&nbsp;{text}" if seconds < 0 else f"{text}&nbsp;ago"
    longtime = date_format(datetime, format="DATETIME_FORMAT", use_l10n=True)
    return mark_safe(f'<span title="{escape(longtime)}">{shorttime}</span>')
