# Generated by Django 3.2.12 on 2022-03-23 20:40

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("toosimpleq", "0009_auto_20210902_2245"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="Schedule",
            new_name="ScheduleExec",
        ),
        migrations.RenameModel(
            old_name="Task",
            new_name="TaskExec",
        ),
    ]