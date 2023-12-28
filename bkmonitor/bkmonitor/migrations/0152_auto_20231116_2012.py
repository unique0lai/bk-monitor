# Generated by Django 3.2.15 on 2023-11-16 12:12

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('bkmonitor', '0151_auto_20231113_1718'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='dutyrule',
            options={'ordering': ['-update_time'], 'verbose_name': '轮值规则', 'verbose_name_plural': '轮值规则'},
        ),
        migrations.AddField(
            model_name='dutyplan',
            name='user_index',
            field=models.IntegerField(default=0, verbose_name='轮班用户的分组'),
        ),
    ]