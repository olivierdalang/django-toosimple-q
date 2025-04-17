from django.contrib import admin
from django.contrib.admin.models import CHANGE, LogEntry
from django.contrib.contenttypes.models import ContentType
from django.contrib.messages.constants import SUCCESS
from django.db.models import F
from django.db.models.functions import Coalesce
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
        "timestamp_",
        "execution_time_",
        "replaced_by_",
        "result_preview",
        "task_",
    ]
    list_display_links = ["task_name"]
    list_filter = ["task_name", TaskQueueListFilter, "state"]
    actions = ["action_requeue"]
    ordering = ["-created"]
    readonly_fields = ["task_", "result"]
    fieldsets = [
        (
            None,
            {"fields": ["icon", "task_name", "state", "task_"]},
        ),
        (
            "Arguments",
            {"fields": ["args", "kwargs"]},
        ),
        (
            "Time",
            {"fields": ["due_", "created_", "started_", "finished_"]},
        ),
        (
            "Retries",
            {"fields": ["retries", "retry_delay", "replaced_by"]},
        ),
        (
            "Execution",
            {"fields": ["worker", "error"]},
        ),
        (
            "Output",
            {"fields": ["stdout", "stderr", "result"]},
        ),
    ]

    def get_queryset(self, request):
        # defer stdout, stderr and results which may host large values
        qs = super().get_queryset(request)
        qs = qs.defer("stdout", "stderr", "result")
        # aggregate time for an unique field
        qs = qs.annotate(
            sortable_time=Coalesce("finished", "started", "due", "created"),
            execution_time=F("finished") - F("started"),
        )
        return qs

    def arguments_(self, obj):
        return format_html(
            "{}<br/>{}",
            truncatechars(str(obj.args), 32),
            truncatechars(str(obj.kwargs), 32),
        )

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

    @admin.display(ordering="sortable_time")
    def timestamp_(self, obj):
        if obj.finished:
            label = "finished"
        elif obj.started:
            label = "started"
        elif obj.due:
            label = "due"
        else:
            label = "created"
        return mark_safe(f"{short_naturaltime(obj.sortable_time)} [{label}]")

    @admin.display(ordering="execution_time")
    def execution_time_(self, obj):
        if not obj.execution_time:
            return None
        return short_seconds(obj.execution_time.seconds, additional_details=1)

    def replaced_by_(self, obj):
        if obj.replaced_by:
            return f"{obj.replaced_by.icon} [{obj.replaced_by.pk}]"

    def task_(self, obj):
        if not obj.task:
            return None
        return render_to_string("toosimpleq/task.html", {"task": obj.task})

    def get_actions(self, request):
        actions = super().get_actions(request)
        if not request.user.has_perm("toosimpleq.requeue_taskexec"):
            actions.pop("action_requeue", None)
        return actions

    @admin.display(description="Requeue task")
    def action_requeue(self, request, queryset):
        for task in queryset:
            new_task = tasks_registry[task.task_name].enqueue(*task.args, **task.kwargs)
            LogEntry.objects.log_action(
                user_id=request.user.id,
                content_type_id=ContentType.objects.get_for_model(task).pk,
                object_id=task.pk,
                object_repr=str(task),
                action_flag=CHANGE,
                change_message=(
                    f"Requeued task through admin action (new task id: {new_task.pk})"
                ),
            )
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
    fieldsets = [
        (
            None,
            {"fields": ["icon", "name", "state", "schedule_"]},
        ),
        (
            "Time",
            {"fields": ["last_due_", "next_due_"]},
        ),
        (
            "Execution",
            {"fields": ["last_task_"]},
        ),
    ]

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
        # for schedule not in the code anymore
        if not obj.schedule:
            return "invalid"

        if len(obj.past_dues) >= 1:
            next_due = obj.past_dues[0]
        else:
            next_due = obj.upcomming_due

        if next_due is None:
            return "never"

        formatted_next_due = short_naturaltime(next_due)
        if len(obj.past_dues) > 1:
            formatted_next_due += mark_safe(f" [×{len(obj.past_dues)}]")
        if next_due < timezone.now():
            return mark_safe(f"<span style='color: red'>{formatted_next_due}</span>")
        return formatted_next_due

    def get_actions(self, request):
        actions = super().get_actions(request)
        if not request.user.has_perm("toosimpleq.force_run_scheduleexec"):
            actions.pop("action_force_run", None)
        return actions

    @admin.display(description="Force run schedule")
    def action_force_run(self, request, queryset):
        for schedule_exec in queryset:
            LogEntry.objects.log_action(
                user_id=request.user.id,
                content_type_id=ContentType.objects.get_for_model(schedule_exec).pk,
                object_id=schedule_exec.pk,
                object_repr=str(schedule_exec),
                action_flag=CHANGE,
                change_message=("Forced schedule execution through admin action"),
            )
            schedule_exec.schedule.execute(dues=[None])
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
    fieldsets = [
        (
            None,
            {"fields": ["icon", "label"]},
        ),
        (
            "Queues",
            {"fields": ["included_queues", "excluded_queues"]},
        ),
        (
            "Time",
            {"fields": ["timeout", "last_tick_", "started_", "stopped_"]},
        ),
        (
            "Exit state",
            {"fields": ["exit_code", "exit_log"]},
        ),
    ]

    @admin.display(ordering="last_tick")
    def last_tick_(self, obj):
        return short_naturaltime(obj.last_tick)

    @admin.display(ordering="started")
    def started_(self, obj):
        return short_naturaltime(obj.started)

    @admin.display(ordering="stopped")
    def stopped_(self, obj):
        return short_naturaltime(obj.stopped)


def short_seconds(seconds, additional_details=0):
    if seconds is None:
        return None
    disps = [
        (60, "second"),
        (60 * 60, "minute"),
        (60 * 60 * 24, "hour"),
        (60 * 60 * 24 * 7, "day"),
        (60 * 60 * 24 * 30, "week"),
        (60 * 60 * 24 * 365, "month"),
        (float("inf"), "year"),
    ]
    last_v = 1
    for threshold, abbr in disps:
        if abs(seconds) < threshold:
            count = int(abs(seconds) // last_v)
            plural = "s" if count > 1 else ""
            text = f"{count} {abbr}{plural}"
            if additional_details:
                remainder = seconds - count * last_v
                if remainder > 0:
                    text += " " + short_seconds(remainder, additional_details - 1)
            return text
        last_v = threshold


def short_naturaltime(datetime):
    if datetime is None:
        return None
    seconds = (timezone.now() - datetime).total_seconds()
    text = short_seconds(seconds)
    shorttime = f"in&nbsp;{text}" if seconds < 0 else f"{text}&nbsp;ago"
    longtime = date_format(datetime, format="DATETIME_FORMAT", use_l10n=True)
    return mark_safe(f'<span title="{escape(longtime)}">{shorttime}</span>')
