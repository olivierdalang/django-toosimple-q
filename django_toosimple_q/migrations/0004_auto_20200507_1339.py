# Generated by Django 3.0.6 on 2020-05-07 11:39

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('toosimpleq', '0003_task_queue'),
    ]

    operations = [
        migrations.AlterField(
            model_name='schedule',
            name='name',
            field=models.CharField(max_length=1024, unique=True),
        ),
    ]
