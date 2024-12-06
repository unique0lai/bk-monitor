# Generated by Django 3.2.15 on 2024-12-02 13:10

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('metadata', '0199_esfieldqueryaliasoption'),
    ]

    operations = [
        migrations.CreateModel(
            name='BkAppSpaceRecord',
            fields=[
                ('creator', models.CharField(max_length=64, verbose_name='创建者')),
                ('create_time', models.DateTimeField(auto_now_add=True, verbose_name='创建时间')),
                ('updater', models.CharField(max_length=64, verbose_name='更新者')),
                ('update_time', models.DateTimeField(auto_now=True, verbose_name='更新时间')),
                ('record_id', models.BigAutoField(primary_key=True, serialize=False)),
                ('bk_app_code', models.CharField(max_length=255, verbose_name='蓝鲸应用app_code')),
                ('space_uid', models.CharField(max_length=255, verbose_name='空间UID')),
                ('is_enable', models.BooleanField(default=True, verbose_name='是否启用')),
            ],
            options={
                'verbose_name': '蓝鲸应用空间授权记录',
                'verbose_name_plural': '蓝鲸应用空间授权记录',
                'unique_together': {('bk_app_code', 'space_uid')},
            },
        ),
    ]
