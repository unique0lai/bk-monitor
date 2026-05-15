# Generated manually on 2026-05-16

from django.db import migrations, models

import bkmonitor.utils.db.fields


class Migration(migrations.Migration):
    dependencies = [
        ("metadata", "0260_basereportsinkconfig"),
    ]

    operations = [
        migrations.CreateModel(
            name="RecordRuleV4",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("creator", models.CharField(max_length=64, verbose_name="创建者")),
                ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="创建时间")),
                ("updater", models.CharField(max_length=64, verbose_name="更新者")),
                ("updated_at", models.DateTimeField(auto_now=True, verbose_name="更新时间")),
                ("space_type", models.CharField(max_length=64, verbose_name="空间类型")),
                ("space_id", models.CharField(max_length=128, verbose_name="空间ID")),
                (
                    "bk_tenant_id",
                    models.CharField(default="system", max_length=256, null=True, verbose_name="租户ID"),
                ),
                ("record_name", models.CharField(max_length=128, verbose_name="预计算名称")),
                ("table_id", models.CharField(max_length=128, verbose_name="结果表名")),
                ("dst_vm_table_id", models.CharField(max_length=128, verbose_name="VM 结果表RT")),
                ("input_type", models.CharField(max_length=32, verbose_name="输入类型")),
                (
                    "input_config",
                    bkmonitor.utils.db.fields.JsonField(default=dict, verbose_name="用户原始输入"),
                ),
                (
                    "check_result",
                    bkmonitor.utils.db.fields.JsonField(default=dict, verbose_name="unify-query check 结果"),
                ),
                ("metricql", bkmonitor.utils.db.fields.JsonField(default=list, verbose_name="MetricQL列表")),
                (
                    "src_vm_table_ids",
                    bkmonitor.utils.db.fields.JsonField(default=list, verbose_name="源 VM 结果表列表"),
                ),
                ("route_info", bkmonitor.utils.db.fields.JsonField(default=list, verbose_name="路由信息")),
                ("metric_name", models.CharField(max_length=128, verbose_name="输出指标名")),
                ("labels", bkmonitor.utils.db.fields.JsonField(default=list, verbose_name="附加标签")),
                ("interval", models.CharField(default="1min", max_length=16, verbose_name="计算周期")),
                ("vm_cluster_id", models.IntegerField(blank=True, null=True, verbose_name="VM 集群 ID")),
                ("vm_storage_name", models.CharField(default="", max_length=128, verbose_name="VM 存储名称")),
                ("flow_name", models.CharField(max_length=128, verbose_name="V4 Flow 名称")),
                ("flow_config", bkmonitor.utils.db.fields.JsonField(default=dict, verbose_name="V4 Flow 配置")),
                ("status", models.CharField(default="created", max_length=32, verbose_name="状态")),
                ("last_error", models.TextField(blank=True, default="", verbose_name="最近错误")),
                (
                    "last_trace_id",
                    models.CharField(blank=True, default="", max_length=128, verbose_name="最近 trace_id"),
                ),
                ("last_check_time", models.DateTimeField(blank=True, null=True, verbose_name="最近检查时间")),
                ("last_refresh_time", models.DateTimeField(blank=True, null=True, verbose_name="最近刷新时间")),
                ("auto_refresh", models.BooleanField(default=True, verbose_name="是否自动刷新")),
                ("has_change", models.BooleanField(default=False, verbose_name="是否存在待更新变更")),
                ("refresh_interval", models.IntegerField(default=3600, verbose_name="刷新间隔(秒)")),
            ],
            options={
                "verbose_name": "V4 预计算规则",
                "verbose_name_plural": "V4 预计算规则",
                "unique_together": {
                    ("bk_tenant_id", "space_type", "space_id", "record_name"),
                    ("bk_tenant_id", "table_id"),
                    ("bk_tenant_id", "flow_name"),
                },
            },
        ),
    ]
