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
from abc import ABC, abstractmethod

from django.conf import settings

from metadata.models.record_rule.constants import (
    RECORD_RULE_V4_BKBASE_NAMESPACE,
    RECORD_RULE_V4_BKMONITOR_NAMESPACE,
    RECORD_RULE_V4_DEFAULT_TENANT,
    RecordRuleV4DeploymentStrategy,
)
from metadata.models.record_rule.v4.deployment.plan import FlowPlan
from metadata.models.record_rule.v4.models import (
    RecordRuleV4,
    RecordRuleV4Resolved,
    RecordRuleV4ResolvedRecord,
    RecordRuleV4Spec,
    stable_hash,
)


class DeploymentStrategy(ABC):
    """根据 resolved records 生成目标 Flow 定义。"""

    strategy: str

    @abstractmethod
    def build_flows(
        self, *, rule: RecordRuleV4, spec: RecordRuleV4Spec, resolved: RecordRuleV4Resolved
    ) -> list[FlowPlan]:
        raise NotImplementedError

    def compose_flow_plan(
        self,
        *,
        rule: RecordRuleV4,
        spec: RecordRuleV4Spec,
        flow_key: str,
        flow_name: str,
        records: list[RecordRuleV4ResolvedRecord],
    ) -> FlowPlan:
        flow_config = self.compose_flow_config(rule=rule, spec=spec, flow_name=flow_name, records=records)
        content_hash = stable_hash(
            {
                "flow_key": flow_key,
                "flow_name": flow_name,
                "record_hashes": [record.content_hash for record in records],
                "flow_config": self.strip_runtime_status(flow_config),
            }
        )
        return FlowPlan(
            flow_key=flow_key,
            flow_name=flow_name,
            resolved_records=records,
            flow_config=flow_config,
            content_hash=content_hash,
        )

    @staticmethod
    def compose_flow_config(
        *,
        rule: RecordRuleV4,
        spec: RecordRuleV4Spec,
        flow_name: str,
        records: list[RecordRuleV4ResolvedRecord],
    ) -> dict:
        src_vm_table_ids = sorted({table_id for record in records for table_id in record.src_vm_table_ids})
        source_nodes: list[dict] = []
        source_names: list[str] = []
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

        recording_rule_config: list[dict] = []
        vm_storage_name = ""
        for resolved_record in records:
            spec_record = resolved_record.spec_record
            vm_storage_name = vm_storage_name or resolved_record.vm_storage_name
            for index, expr in enumerate(resolved_record.metricql):
                metric_name = spec_record.metric_name if index == 0 else f"{spec_record.metric_name}_{index + 1}"
                recording_rule_config.append(
                    {
                        "expr": expr,
                        "interval": spec_record.interval,
                        "metric_name": metric_name,
                        "labels": spec_record.labels,
                    }
                )

        return {
            "kind": "Flow",
            "metadata": {
                "tenant": RECORD_RULE_V4_DEFAULT_TENANT,
                "namespace": RECORD_RULE_V4_BKBASE_NAMESPACE,
                "name": flow_name,
                "labels": {},
                "annotations": {},
            },
            "spec": {
                "nodes": [
                    *source_nodes,
                    {
                        "kind": "RecordingRuleNode",
                        "name": flow_name,
                        "inputs": source_names,
                        "output": rule.dst_vm_table_id,
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
                "desired_status": spec.desired_status,
            },
            "status": None,
        }

    @staticmethod
    def strip_runtime_status(flow_config: dict) -> dict:
        pure_config = copy.deepcopy(flow_config)
        pure_config.get("spec", {}).pop("desired_status", None)
        return pure_config


class PerRecordDeploymentStrategy(DeploymentStrategy):
    strategy = RecordRuleV4DeploymentStrategy.PER_RECORD.value

    def build_flows(
        self, *, rule: RecordRuleV4, spec: RecordRuleV4Spec, resolved: RecordRuleV4Resolved
    ) -> list[FlowPlan]:
        flows: list[FlowPlan] = []
        for record in resolved.get_records():
            suffix = record.record_key.rsplit("_", 1)[-1]
            flow_name = RecordRuleV4.compose_flow_name(
                rule.group_name, record.spec_record.record_name, random_suffix=suffix
            )
            flows.append(
                self.compose_flow_plan(
                    rule=rule,
                    spec=spec,
                    flow_key=record.record_key,
                    flow_name=flow_name,
                    records=[record],
                )
            )
        return flows


class SingleFlowDeploymentStrategy(DeploymentStrategy):
    strategy = RecordRuleV4DeploymentStrategy.SINGLE_FLOW.value

    def build_flows(
        self, *, rule: RecordRuleV4, spec: RecordRuleV4Spec, resolved: RecordRuleV4Resolved
    ) -> list[FlowPlan]:
        records = resolved.get_records()
        if not records:
            return []
        table_name = rule.table_id.split(".", 1)[0]
        suffix = table_name.rsplit("_", 1)[-1]
        flow_name = RecordRuleV4.compose_flow_name(rule.group_name, "group", random_suffix=suffix)
        return [
            self.compose_flow_plan(
                rule=rule,
                spec=spec,
                flow_key="group",
                flow_name=flow_name,
                records=records,
            )
        ]


def get_deployment_strategy(strategy: str) -> DeploymentStrategy:
    if strategy == RecordRuleV4DeploymentStrategy.SINGLE_FLOW.value:
        return SingleFlowDeploymentStrategy()
    return PerRecordDeploymentStrategy()
