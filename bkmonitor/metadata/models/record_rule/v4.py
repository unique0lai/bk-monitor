"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import copy
import logging
from typing import Any

from django.conf import settings
from django.db import models
from django.db.transaction import atomic
from django.utils import timezone

from bkmonitor.utils.db import JsonField
from bkmonitor.utils.tenant import space_uid_to_bk_tenant_id
from core.drf_resource import api
from metadata import config
from metadata.models.common import BaseModelWithTime
from metadata.models.record_rule import utils
from metadata.models.record_rule.constants import (
    RECORD_RULE_V4_BKBASE_NAMESPACE,
    RECORD_RULE_V4_BKMONITOR_NAMESPACE,
    RECORD_RULE_V4_DEFAULT_REFRESH_INTERVAL,
    RECORD_RULE_V4_DEFAULT_TENANT,
    RECORD_RULE_V4_INTERVAL_CHOICES,
    RecordRuleV4InputType,
    RecordRuleV4Status,
)

logger = logging.getLogger("metadata")


class RecordRuleV4(BaseModelWithTime):
    """V4 Recording Rule 预计算规则"""

    space_type = models.CharField("空间类型", max_length=64)
    space_id = models.CharField("空间ID", max_length=128)
    bk_tenant_id = models.CharField("租户ID", max_length=256, null=True, default="system")

    record_name = models.CharField("预计算名称", max_length=128)
    table_id = models.CharField("结果表名", max_length=128)
    dst_vm_table_id = models.CharField("VM 结果表RT", max_length=128)

    input_type = models.CharField("输入类型", max_length=32)
    input_config = JsonField("用户原始输入", default=dict)
    check_result = JsonField("unify-query check 结果", default=dict)

    metricql = JsonField("MetricQL列表", default=list)
    src_vm_table_ids = JsonField("源 VM 结果表列表", default=list)
    route_info = JsonField("路由信息", default=list)

    metric_name = models.CharField("输出指标名", max_length=128)
    labels = JsonField("附加标签", default=list)
    interval = models.CharField("计算周期", max_length=16, default="1min")
    vm_cluster_id = models.IntegerField("VM 集群 ID", null=True, blank=True)
    vm_storage_name = models.CharField("VM 存储名称", max_length=128, default="")

    flow_name = models.CharField("V4 Flow 名称", max_length=128)
    flow_config = JsonField("V4 Flow 配置", default=dict)
    status = models.CharField("状态", max_length=32, default=RecordRuleV4Status.CREATED.value)
    last_error = models.TextField("最近错误", blank=True, default="")
    last_trace_id = models.CharField("最近 trace_id", max_length=128, blank=True, default="")
    last_check_time = models.DateTimeField("最近检查时间", null=True, blank=True)
    last_refresh_time = models.DateTimeField("最近刷新时间", null=True, blank=True)

    auto_refresh = models.BooleanField("是否自动刷新", default=True)
    has_change = models.BooleanField("是否存在待更新变更", default=False)
    refresh_interval = models.IntegerField("刷新间隔(秒)", default=RECORD_RULE_V4_DEFAULT_REFRESH_INTERVAL)

    class Meta:
        verbose_name = "V4 预计算规则"
        verbose_name_plural = "V4 预计算规则"
        unique_together = (
            ("bk_tenant_id", "space_type", "space_id", "record_name"),
            ("bk_tenant_id", "table_id"),
            ("bk_tenant_id", "flow_name"),
        )

    @property
    def space_uid(self) -> str:
        return f"{self.space_type}__{self.space_id}"

    @classmethod
    def compose_table_id(cls, space_type: str, space_id: str, record_name: str) -> str:
        return utils.generate_pre_cal_table_id(space_type=space_type, space_id=space_id, record_name=record_name)

    @classmethod
    def compose_dst_vm_table_id(cls, table_id: str) -> str:
        return f"{settings.DEFAULT_BKDATA_BIZ_ID}_{utils.compose_rule_table_id(table_id)}"

    @classmethod
    def compose_flow_name(cls, table_id: str) -> str:
        return utils.compose_rule_table_id(table_id)

    @classmethod
    @atomic(config.DATABASE_CONNECTION_NAME)
    def create_rule(
        cls,
        space_type: str,
        space_id: str,
        record_name: str,
        input_type: str,
        input_config: dict[str, Any],
        metric_name: str,
        labels: list[dict[str, str]] | None = None,
        interval: str = "1min",
        bk_tenant_id: str | None = None,
        refresh_interval: int = RECORD_RULE_V4_DEFAULT_REFRESH_INTERVAL,
        auto_refresh: bool = True,
    ) -> "RecordRuleV4":
        cls.validate_input_type(input_type)
        cls.validate_interval(interval)
        bk_tenant_id = bk_tenant_id or space_uid_to_bk_tenant_id(f"{space_type}__{space_id}")
        table_id = cls.compose_table_id(space_type, space_id, record_name)
        dst_vm_table_id = cls.compose_dst_vm_table_id(table_id)
        flow_name = cls.compose_flow_name(table_id)

        rule = cls(
            bk_tenant_id=bk_tenant_id,
            space_type=space_type,
            space_id=space_id,
            record_name=record_name,
            table_id=table_id,
            dst_vm_table_id=dst_vm_table_id,
            input_type=input_type,
            input_config=input_config,
            metric_name=metric_name,
            labels=labels or [],
            interval=interval,
            flow_name=flow_name,
            refresh_interval=refresh_interval,
            auto_refresh=auto_refresh,
        )
        check_result = rule.run_check()
        runtime_config = rule.build_runtime_config(check_result)
        rule.fill_runtime_config(check_result, runtime_config)
        rule.ensure_output_result_table()
        rule.apply_flow()
        rule.status = RecordRuleV4Status.RUNNING.value
        rule.last_error = ""
        rule.has_change = False
        rule.save()
        return rule

    @staticmethod
    def validate_input_type(input_type: str) -> None:
        if input_type not in {item.value for item in RecordRuleV4InputType}:
            raise ValueError(f"unsupported input_type: {input_type}")

    @staticmethod
    def validate_interval(interval: str) -> None:
        if interval not in RECORD_RULE_V4_INTERVAL_CHOICES:
            raise ValueError(f"unsupported interval: {interval}")

    def run_check(self) -> dict:
        params = copy.deepcopy(self.input_config or {})
        if self.input_type == RecordRuleV4InputType.QUERY_TS.value:
            params.setdefault("space_uid", self.space_uid)
            result = api.unify_query.check_query_ts(bk_tenant_id=self.bk_tenant_id, **params)
        elif self.input_type == RecordRuleV4InputType.PROMQL.value:
            result = api.unify_query.check_query_ts_by_promql(bk_tenant_id=self.bk_tenant_id, **params)
        else:
            raise ValueError(f"unsupported input_type: {self.input_type}")
        return result or {}

    def build_runtime_config(self, check_result: dict) -> dict:
        trace_id = check_result.get("trace_id", "")
        route_info = check_result.get("route_info") or []
        data = check_result.get("data") or []
        if not route_info:
            raise ValueError(f"unify-query check route_info is empty, trace_id: {trace_id}")

        metricql = self.extract_metricql(data)
        if not metricql:
            raise ValueError(f"unify-query check metricql is empty, trace_id: {trace_id}")

        src_vm_table_ids = self.normalize_src_vm_table_ids(self.extract_src_vm_table_ids(data, route_info))
        if not src_vm_table_ids:
            raise ValueError(f"unify-query check src vm table ids is empty, trace_id: {trace_id}")

        vm_storage_info = self.get_vm_storage_info()
        flow_config = self.compose_flow_config(
            metricql=metricql, src_vm_table_ids=src_vm_table_ids, vm_storage_name=vm_storage_info["cluster_name"]
        )
        return {
            "metricql": metricql,
            "src_vm_table_ids": src_vm_table_ids,
            "route_info": route_info,
            "vm_cluster_id": vm_storage_info["cluster_id"],
            "vm_storage_name": vm_storage_info["cluster_name"],
            "flow_config": flow_config,
        }

    @staticmethod
    def extract_metricql(data: list[dict]) -> list[str]:
        metricql = []
        for item in data:
            value = item.get("metricql")
            if value and value not in metricql:
                metricql.append(value)
        return metricql

    @staticmethod
    def extract_src_vm_table_ids(data: list[dict], route_info: list[dict]) -> list[str]:
        table_ids = []
        for item in data:
            result_table_id = item.get("result_table_id") or []
            if isinstance(result_table_id, str):
                result_table_id = [result_table_id]
            for table_id in result_table_id:
                if table_id and table_id not in table_ids:
                    table_ids.append(table_id)
        for item in route_info:
            table_id = item.get("table_id")
            if table_id and table_id not in table_ids:
                table_ids.append(table_id)
        return sorted(table_ids)

    def normalize_src_vm_table_ids(self, table_ids: list[str]) -> list[str]:
        from metadata import models as metadata_models

        exclude_table_ids = {self.table_id, self.dst_vm_table_id}
        vm_records = metadata_models.AccessVMRecord.objects.filter(bk_tenant_id=self.bk_tenant_id).filter(
            models.Q(vm_result_table_id__in=table_ids) | models.Q(result_table_id__in=table_ids)
        )
        vm_map = {}
        for record in vm_records:
            vm_map[record.vm_result_table_id] = record.vm_result_table_id
            vm_map[record.result_table_id] = record.vm_result_table_id

        result, missing = [], []
        for table_id in table_ids:
            vm_table_id = vm_map.get(table_id)
            if not vm_table_id:
                missing.append(table_id)
                continue
            if table_id in exclude_table_ids or vm_table_id in exclude_table_ids:
                logger.info(
                    "RecordRuleV4 normalize_src_vm_table_ids: skip self reference table_id->[%s], "
                    "vm_table_id->[%s], rule_table_id->[%s]",
                    table_id,
                    vm_table_id,
                    self.table_id,
                )
                continue
            if vm_table_id not in result:
                result.append(vm_table_id)
        if missing:
            raise ValueError(f"source result tables are not access vm storage: {missing}")
        return sorted(result)

    def get_vm_storage_info(self) -> dict:
        from metadata.models.vm import utils as vm_utils

        return vm_utils.get_vm_cluster_id_name(
            bk_tenant_id=self.bk_tenant_id, space_type=self.space_type, space_id=self.space_id
        )

    def compose_flow_config(self, metricql: list[str], src_vm_table_ids: list[str], vm_storage_name: str) -> dict:
        source_nodes = []
        source_names = []
        for index, table_id in enumerate(src_vm_table_ids):
            name = "vm_source" if len(src_vm_table_ids) == 1 else f"vm_source_{index + 1}"
            source_names.append(name)
            source_nodes.append(
                {
                    "kind": "VmSourceNode",
                    "name": name,
                    "data": {
                        "kind": "ResultTable",
                        "tenant": RECORD_RULE_V4_DEFAULT_TENANT,
                        "namespace": RECORD_RULE_V4_BKMONITOR_NAMESPACE,
                        "name": table_id,
                    },
                }
            )

        recording_rule_config = []
        for index, expr in enumerate(metricql):
            metric_name = self.metric_name if index == 0 else f"{self.metric_name}_{index + 1}"
            recording_rule_config.append(
                {"expr": expr, "interval": self.interval, "metric_name": metric_name, "labels": self.labels}
            )

        return {
            "kind": "Flow",
            "metadata": {
                "tenant": RECORD_RULE_V4_DEFAULT_TENANT,
                "namespace": RECORD_RULE_V4_BKBASE_NAMESPACE,
                "name": self.flow_name,
                "labels": {},
                "annotations": {},
            },
            "spec": {
                "nodes": [
                    *source_nodes,
                    {
                        "kind": "RecordingRuleNode",
                        "name": self.flow_name,
                        "inputs": source_names,
                        "output": self.dst_vm_table_id,
                        "config": recording_rule_config,
                        "storage": {
                            "kind": "VmStorage",
                            "tenant": RECORD_RULE_V4_DEFAULT_TENANT,
                            "namespace": RECORD_RULE_V4_BKMONITOR_NAMESPACE,
                            "name": vm_storage_name,
                        },
                    },
                ],
                "operation_config": {
                    "start_position": "from_head",
                    "stream_cluster": None,
                    "batch_cluster": None,
                    "deploy_mode": None,
                },
                "maintainers": [settings.BK_DATA_PROJECT_MAINTAINER],
                "desired_status": "running",
            },
            "status": None,
        }

    def fill_runtime_config(self, check_result: dict, runtime_config: dict) -> None:
        self.check_result = check_result
        self.metricql = runtime_config["metricql"]
        self.src_vm_table_ids = runtime_config["src_vm_table_ids"]
        self.route_info = runtime_config["route_info"]
        self.vm_cluster_id = runtime_config["vm_cluster_id"]
        self.vm_storage_name = runtime_config["vm_storage_name"]
        self.flow_config = runtime_config["flow_config"]
        self.last_trace_id = check_result.get("trace_id", "")
        self.last_check_time = timezone.now()

    def ensure_output_result_table(self) -> None:
        from metadata import models as metadata_models

        biz_id = metadata_models.Space.objects.get_biz_id_by_space(self.space_type, self.space_id)
        metadata_models.ResultTable.objects.get_or_create(
            bk_tenant_id=self.bk_tenant_id,
            table_id=self.table_id,
            defaults={
                "table_name_zh": self.table_id,
                "is_custom_table": True,
                "default_storage": metadata_models.ClusterInfo.TYPE_VM,
                "creator": "system",
                "bk_biz_id": biz_id,
            },
        )
        metadata_models.ResultTableField.objects.get_or_create(
            bk_tenant_id=self.bk_tenant_id,
            table_id=self.table_id,
            field_name=self.metric_name,
            defaults={
                "field_type": metadata_models.ResultTableField.FIELD_TYPE_FLOAT,
                "description": self.metric_name,
                "tag": metadata_models.ResultTableField.FIELD_TAG_METRIC,
                "is_config_by_user": True,
            },
        )
        metadata_models.AccessVMRecord.objects.get_or_create(
            bk_tenant_id=self.bk_tenant_id,
            result_table_id=self.table_id,
            defaults={
                "bk_base_data_id": 0,
                "vm_result_table_id": self.dst_vm_table_id,
                "vm_cluster_id": self.vm_cluster_id,
            },
        )

    def apply_flow(self) -> None:
        api.bkdata.apply_data_link(config=[self.flow_config])
        self.last_refresh_time = timezone.now()

    def delete_flow(self) -> None:
        api.bkdata.delete_data_link(
            bk_tenant_id=self.bk_tenant_id,
            namespace=RECORD_RULE_V4_BKBASE_NAMESPACE,
            kind="flows",
            name=self.flow_name,
        )

    @atomic(config.DATABASE_CONNECTION_NAME)
    def modify_rule(self, **kwargs) -> "RecordRuleV4":
        for field in ["input_type", "input_config", "metric_name", "labels", "interval", "auto_refresh"]:
            if field not in kwargs:
                continue
            setattr(self, field, kwargs[field])
        self.validate_input_type(self.input_type)
        self.validate_interval(self.interval)
        check_result = self.run_check()
        runtime_config = self.build_runtime_config(check_result)
        self.fill_runtime_config(check_result, runtime_config)
        self.ensure_output_result_table()
        self.apply_flow()
        self.status = RecordRuleV4Status.RUNNING.value
        self.last_error = ""
        self.has_change = False
        self.save()
        return self

    @atomic(config.DATABASE_CONNECTION_NAME)
    def refresh_if_changed(self, auto_apply: bool | None = None) -> bool:
        try:
            check_result = self.run_check()
            runtime_config = self.build_runtime_config(check_result)
        except Exception as err:
            self.last_error = str(err)
            self.last_check_time = timezone.now()
            self.save(update_fields=["last_error", "last_check_time", "updated_at"])
            logger.exception("RecordRuleV4 refresh check failed, id: %s", self.pk)
            return False

        changed = any(
            [
                self.metricql != runtime_config["metricql"],
                self.src_vm_table_ids != runtime_config["src_vm_table_ids"],
                self.vm_storage_name != runtime_config["vm_storage_name"],
                self.flow_config != runtime_config["flow_config"],
            ]
        )

        if auto_apply is None:
            auto_apply = self.auto_refresh

        if changed and not auto_apply:
            self.has_change = True
            self.last_error = ""
            self.last_trace_id = check_result.get("trace_id", "")
            self.last_check_time = timezone.now()
            self.save(update_fields=["has_change", "last_error", "last_trace_id", "last_check_time", "updated_at"])
            return True

        self.fill_runtime_config(check_result, runtime_config)
        self.has_change = False
        if changed:
            self.apply_flow()
            self.last_error = ""
        self.save()
        return changed

    def is_refresh_due(self) -> bool:
        if not self.last_check_time:
            return True
        check_before = timezone.now() - timezone.timedelta(
            seconds=self.refresh_interval or RECORD_RULE_V4_DEFAULT_REFRESH_INTERVAL
        )
        return self.last_check_time <= check_before

    @atomic(config.DATABASE_CONNECTION_NAME)
    def mark_deleted(self) -> None:
        self.delete_flow()
        self.status = RecordRuleV4Status.DELETED.value
        self.save(update_fields=["status", "updated_at"])

    def to_dict(self) -> dict:
        return {
            "id": self.pk,
            "bk_tenant_id": self.bk_tenant_id,
            "space_type": self.space_type,
            "space_id": self.space_id,
            "record_name": self.record_name,
            "table_id": self.table_id,
            "dst_vm_table_id": self.dst_vm_table_id,
            "input_type": self.input_type,
            "input_config": self.input_config,
            "check_result": self.check_result,
            "metricql": self.metricql,
            "src_vm_table_ids": self.src_vm_table_ids,
            "route_info": self.route_info,
            "metric_name": self.metric_name,
            "labels": self.labels,
            "interval": self.interval,
            "vm_cluster_id": self.vm_cluster_id,
            "vm_storage_name": self.vm_storage_name,
            "flow_name": self.flow_name,
            "flow_config": self.flow_config,
            "status": self.status,
            "last_error": self.last_error,
            "last_trace_id": self.last_trace_id,
            "last_check_time": self.last_check_time,
            "last_refresh_time": self.last_refresh_time,
            "auto_refresh": self.auto_refresh,
            "has_change": self.has_change,
            "refresh_interval": self.refresh_interval,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
