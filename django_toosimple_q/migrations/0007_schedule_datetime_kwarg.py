# Generated by Django 3.0.6 on 2021-06-22 17:08

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("toosimpleq", "0006_task_replacement"),
    ]

    operations = [
        migrations.AddField(
            model_name="schedule",
            name="datetime_kwarg",
            field=models.CharField(blank=True, max_length=1024, null=True),
        ),
    ]
