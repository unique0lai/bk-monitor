"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

from django.db import models, transaction

from core.drf_resource import api
from metadata.models.record_rule.constants import (
    RecordRuleV4DesiredStatus,
    RecordRuleV4InputType,
)
from metadata.models.record_rule.v4.models import (
    CONDITION_FALSE,
    CONDITION_RESOLVED,
    CONDITION_TRUE,
    CONDITION_UPDATE_AVAILABLE,
    RecordRuleV4,
    RecordRuleV4Event,
    RecordRuleV4Resolved,
    RecordRuleV4ResolvedRecord,
    RecordRuleV4Spec,
    RecordRuleV4SpecRecord,
    merge_labels,
    now,
    stable_hash,
)

logger = logging.getLogger("metadata")


class RecordRuleV4Resolver:
    """将当前 spec 解析为 resolved 快照。

    Resolver 的职责边界是调用 unify-query check，并把 check 结果沉淀成
    Resolved / ResolvedRecord。是否生成 Flow、是否下发都不在这里处理。
    """

    def __init__(self, rule: RecordRuleV4, source: str = "system", operator: str = "") -> None:
        self.rule = rule
        self.source = source
        self.operator = operator

    @property
    def actor(self) -> str:
        return self.operator or self.source

    def reload_rule(self, for_update: bool = False) -> RecordRuleV4:
        """重新加载 rule，避免 resolve 长流程使用过期指针。"""

        queryset = RecordRuleV4.objects
        if for_update:
            queryset = queryset.select_for_update()
        self.rule = queryset.get(pk=self.rule.pk)
        return self.rule

    def resolve_current(self, force: bool = False) -> RecordRuleV4Resolved | None:
        """解析当前 spec，语义变化时创建新的 resolved 快照。"""

        self.reload_rule()
        spec = self.rule.current_spec
        if spec is None:
            self.rule.set_condition(CONDITION_RESOLVED, CONDITION_FALSE, "SpecMissing", "current spec is missing")
            self.rule.last_error = "current spec is missing"
            self.rule.sync_phase()
            self.rule.save()
            RecordRuleV4Event.record_resolve_failed(
                self.rule, source=self.source, operator=self.operator, message=self.rule.last_error
            )
            return None

        if spec.desired_status == RecordRuleV4DesiredStatus.DELETED.value:
            return None

        try:
            # 外部 check 可能耗时或失败，先在事务外完成，避免长时间持有行锁。
            runtime_records = [
                self.build_runtime_record(record) for record in spec.records.order_by("source_index", "id")
            ]
        except Exception as err:
            if not self.is_spec_current(spec):
                # 用户在 check 过程中更新了 spec，本次结果已经过期，直接丢弃。
                return None
            self.rule.last_error = str(err)
            self.rule.last_check_time = now()
            self.rule.set_condition(CONDITION_RESOLVED, CONDITION_FALSE, "ResolveFailed", str(err))
            self.rule.sync_phase()
            self.rule.save()
            RecordRuleV4Event.record_resolve_failed(
                self.rule,
                spec=spec,
                source=self.source,
                operator=self.operator,
                message=str(err),
            )
            logger.exception("RecordRuleV4 resolve failed, id: %s", self.rule.pk)
            return None

        if not self.is_spec_current(spec):
            return None

        # resolved content_hash 只包含解析语义结果，不包含后续 Flow 模板。
        resolved_config = {"records": [record["resolved_payload"] for record in runtime_records]}
        content_hash = stable_hash(resolved_config)

        with transaction.atomic():
            self.reload_rule(for_update=True)
            if self.rule.current_spec_id != spec.pk or self.rule.generation != spec.generation:
                return None

            latest_resolved = self.rule.latest_resolved
            if not force and latest_resolved and latest_resolved.content_hash == content_hash:
                # 解析语义未变时只更新时间和 condition，不推进 resolved 版本。
                self.rule.last_error = ""
                self.rule.last_check_time = now()
                if (
                    self.rule.applied_deployment_id
                    and self.rule.applied_deployment_id == self.rule.latest_deployment_id
                ):
                    self.rule.observed_generation = max(self.rule.observed_generation, spec.generation)
                    self.rule.update_available = False
                    self.rule.set_condition(CONDITION_UPDATE_AVAILABLE, CONDITION_FALSE, "ResolvedUnchanged")
                self.rule.set_condition(CONDITION_RESOLVED, CONDITION_TRUE, "Unchanged")
                self.rule.sync_phase()
                self.rule.save()
                RecordRuleV4Event.record_resolve_unchanged(
                    self.rule,
                    spec,
                    latest_resolved,
                    source=self.source,
                    operator=self.operator,
                )
                return latest_resolved

            # 解析语义变化才创建新版本，用于后续生成部署计划。
            resolved = RecordRuleV4Resolved.objects.create(
                rule=self.rule,
                spec=spec,
                generation=spec.generation,
                resolve_version=self.next_resolve_version(spec),
                resolved_config=resolved_config,
                content_hash=content_hash,
                source=self.source,
                creator=self.actor,
                updater=self.actor,
            )
            for runtime_record in runtime_records:
                # resolved record 保留每条逻辑 record 的 metricql / VMRT 范围，
                # 后续部署策略只消费这一层结构。
                spec_record = runtime_record["spec_record"]
                RecordRuleV4ResolvedRecord.objects.create(
                    resolved=resolved,
                    spec_record=spec_record,
                    record_key=spec_record.record_key,
                    identity_hash=spec_record.identity_hash,
                    content_hash=runtime_record["content_hash"],
                    source_index=spec_record.source_index,
                    metricql=runtime_record["metricql"],
                    labels=runtime_record["labels"],
                    src_vm_table_ids=runtime_record["src_vm_table_ids"],
                    src_result_table_configs=runtime_record["src_result_table_configs"],
                    route_info=runtime_record["route_info"],
                    vm_cluster_id=runtime_record["vm_cluster_id"],
                    vm_storage_name=runtime_record["vm_storage_name"],
                    creator=self.actor,
                    updater=self.actor,
                )
            self.rule.use_resolved(resolved)
            RecordRuleV4Event.record_resolve_changed(
                self.rule,
                spec,
                resolved,
                source=self.source,
                operator=self.operator,
            )
            return resolved

    def is_spec_current(self, spec: RecordRuleV4Spec) -> bool:
        self.reload_rule()
        return self.rule.current_spec_id == spec.pk and self.rule.generation == spec.generation

    def build_runtime_record(self, spec_record: RecordRuleV4SpecRecord) -> dict[str, Any]:
        """将一条 spec record 解析成运行时 record payload。"""

        check_result = self.run_check(spec_record)
        route_info = check_result.get("route_info") or []
        data = check_result.get("data") or []
        if not route_info:
            raise ValueError(f"unify-query check route_info is empty, record_key: {spec_record.record_key}")

        metricql = self.extract_metricql(data)
        if not metricql:
            raise ValueError(f"unify-query check metricql is empty, record_key: {spec_record.record_key}")

        src_vm_table_ids = self.normalize_src_vm_table_ids(self.extract_src_vm_table_ids(data, route_info))
        if not src_vm_table_ids:
            raise ValueError(f"unify-query check src vm table ids is empty, record_key: {spec_record.record_key}")
        src_result_table_configs = self.resolve_src_result_table_configs(src_vm_table_ids)
        labels = merge_labels(spec_record.spec.labels, spec_record.labels)

        # 输出 VM storage 跟当前空间相关，而不是从单条查询结果里推导。
        vm_storage_info = self.get_vm_storage_info()
        resolved_payload = {
            "record_key": spec_record.record_key,
            "metricql": metricql,
            "labels": labels,
            "interval": spec_record.spec.interval,
            "src_vm_table_ids": src_vm_table_ids,
            "src_result_table_configs": src_result_table_configs,
            "route_info": route_info,
            "vm_cluster_id": vm_storage_info["cluster_id"],
            "vm_storage_name": vm_storage_info["cluster_name"],
        }
        return {
            "spec_record": spec_record,
            "metricql": metricql,
            "labels": labels,
            "src_vm_table_ids": src_vm_table_ids,
            "src_result_table_configs": src_result_table_configs,
            "route_info": route_info,
            "vm_cluster_id": vm_storage_info["cluster_id"],
            "vm_storage_name": vm_storage_info["cluster_name"],
            "resolved_payload": resolved_payload,
            "content_hash": stable_hash(resolved_payload),
        }

    def run_check(self, spec_record: RecordRuleV4SpecRecord) -> dict[str, Any]:
        """根据输入类型调用 unify-query 的预览接口。"""

        params: dict[str, Any] = copy.deepcopy(spec_record.input_config or {})
        if spec_record.input_type == RecordRuleV4InputType.QUERY_TS.value:
            params.setdefault("space_uid", self.rule.space_uid)
            result = api.unify_query.check_query_ts(bk_tenant_id=self.rule.bk_tenant_id, **params)
        elif spec_record.input_type == RecordRuleV4InputType.PROMQL.value:
            result = api.unify_query.check_query_ts_by_promql(bk_tenant_id=self.rule.bk_tenant_id, **params)
        else:
            raise ValueError(f"unsupported input_type: {spec_record.input_type}")
        return result or {}

    def next_resolve_version(self, spec: RecordRuleV4Spec) -> int:
        """获取同一 spec 下的下一个解析版本号。"""

        latest = RecordRuleV4Resolved.objects.filter(rule=self.rule, spec=spec).order_by("-resolve_version").first()
        return 1 if latest is None else latest.resolve_version + 1

    @staticmethod
    def extract_metricql(data: list[dict[str, Any]]) -> list[str]:
        """从 check data 中提取去重后的 MetricQL。"""

        metricql: list[str] = []
        for item in data:
            value = item.get("metricql")
            if value and value not in metricql:
                metricql.append(value)
        return metricql

    @staticmethod
    def extract_src_vm_table_ids(data: list[dict[str, Any]], route_info: list[dict[str, Any]]) -> list[str]:
        """从 check data 和 route_info 中合并源结果表。"""

        table_ids: list[str] = []
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
        """把源 RT 统一转换成 VM RT，并排除当前预计算自己的输出表。"""

        from metadata import models as metadata_models

        exclude_table_ids = {self.rule.table_id, self.rule.dst_vm_table_id}
        vm_records = metadata_models.AccessVMRecord.objects.filter(bk_tenant_id=self.rule.bk_tenant_id).filter(
            models.Q(vm_result_table_id__in=table_ids) | models.Q(result_table_id__in=table_ids)
        )
        vm_map: dict[str, str] = {}
        for record in vm_records:
            vm_map[record.vm_result_table_id] = record.vm_result_table_id
            vm_map[record.result_table_id] = record.vm_result_table_id

        result: list[str] = []
        missing: list[str] = []
        for table_id in table_ids:
            vm_table_id = vm_map.get(table_id)
            if not vm_table_id:
                missing.append(table_id)
                continue
            if table_id in exclude_table_ids or vm_table_id in exclude_table_ids:
                # 解析结果可能因为已有预计算链路而包含自身输出；这里必须跳过，
                # 否则会生成自引用的 VmSourceNode。
                logger.info(
                    "RecordRuleV4 normalize_src_vm_table_ids: skip self reference table_id->[%s], "
                    "vm_table_id->[%s], rule_table_id->[%s]",
                    table_id,
                    vm_table_id,
                    self.rule.table_id,
                )
                continue
            if vm_table_id not in result:
                result.append(vm_table_id)
        if missing:
            raise ValueError(f"source result tables are not access vm storage: {missing}")
        return sorted(result)

    def resolve_src_result_table_configs(self, vm_table_ids: list[str]) -> list[dict[str, str]]:
        """把源 VMRT 固化成 bkbase ResultTableConfig.name 快照。"""

        from metadata import models as metadata_models

        result: list[dict[str, str]] = []
        missing_access_records: list[str] = []
        missing_result_table_configs: list[str] = []
        for vm_table_id in vm_table_ids:
            access_record = (
                metadata_models.AccessVMRecord.objects.filter(
                    bk_tenant_id=self.rule.bk_tenant_id,
                    vm_result_table_id=vm_table_id,
                )
                .order_by("-id")
                .first()
            )
            if access_record is None:
                missing_access_records.append(vm_table_id)
                continue

            result_table_configs = metadata_models.ResultTableConfig.objects.filter(
                bk_tenant_id=self.rule.bk_tenant_id,
                table_id=access_record.result_table_id,
            ).order_by("-last_modify_time", "-id")
            config_count = result_table_configs.count()
            result_table_config = result_table_configs.first()
            if result_table_config is None:
                missing_result_table_configs.append(access_record.result_table_id)
                continue
            if config_count > 1:
                logger.warning(
                    "RecordRuleV4 resolve_src_result_table_configs: got multiple ResultTableConfig, "
                    "table_id->[%s], selected name->[%s]",
                    access_record.result_table_id,
                    result_table_config.name,
                )
            result.append(
                {
                    "result_table_id": access_record.result_table_id,
                    "vm_result_table_id": access_record.vm_result_table_id,
                    "bkbase_result_table_name": result_table_config.name,
                }
            )

        if missing_access_records:
            raise ValueError(f"source vm result tables are not found in AccessVMRecord: {missing_access_records}")
        if missing_result_table_configs:
            raise ValueError(f"source result tables are not found in ResultTableConfig: {missing_result_table_configs}")
        return result

    def get_vm_storage_info(self) -> dict[str, Any]:
        """获取当前空间 recording rule 输出要写入的 VM storage。"""

        from metadata.models.vm import utils as vm_utils

        return vm_utils.get_vm_cluster_id_name(
            bk_tenant_id=self.rule.bk_tenant_id,
            space_type=self.rule.space_type,
            space_id=self.rule.space_id,
        )
