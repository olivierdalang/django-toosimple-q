# Generated by Django 3.2.12 on 2022-03-27 20:22

import datetime

import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("toosimpleq", "0010_auto_20220324_0419"),
    ]

    operations = [
        migrations.CreateModel(
            name="WorkerStatus",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("label", models.CharField(max_length=1024, unique=True)),
                ("included_queues", models.JSONField(default=list)),
                ("excluded_queues", models.JSONField(default=list)),
                (
                    "timeout",
                    models.DurationField(default=datetime.timedelta(seconds=3600)),
                ),
                ("last_tick", models.DateTimeField(default=django.utils.timezone.now)),
                ("started", models.DateTimeField(default=django.utils.timezone.now)),
                ("stopped", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "verbose_name": "Worker Status",
                "verbose_name_plural": "Workers Statuses",
            },
        ),
        migrations.AddField(
            model_name="taskexec",
            name="worker",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                to="toosimpleq.workerstatus",
            ),
        ),
    ]
