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

from dataclasses import dataclass
from typing import Any

from metadata.models.record_rule.v4.models import RecordRuleV4Flow, RecordRuleV4ResolvedRecord


@dataclass(frozen=True)
class FlowPlan:
    flow_key: str
    flow_name: str
    resolved_records: list[RecordRuleV4ResolvedRecord]
    flow_config: dict[str, Any]
    content_hash: str


@dataclass(frozen=True)
class DeploymentPlanAction:
    action_key: str
    action_type: str
    flow: RecordRuleV4Flow
    content_hash: str
    flow_config_snapshot: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_key": self.action_key,
            "action_type": self.action_type,
            "flow_id": self.flow.pk,
            "flow_key": self.flow.flow_key,
            "flow_name": self.flow.flow_name,
            "content_hash": self.content_hash,
            "flow_config_snapshot": self.flow_config_snapshot,
        }


@dataclass(frozen=True)
class DeploymentPlan:
    strategy: str
    actions: list[DeploymentPlanAction]
    target_flows: list[RecordRuleV4Flow]

    def to_config(self, *, desired_status: str, resolved_content_hash: str) -> dict[str, Any]:
        return {
            "desired_status": desired_status,
            "strategy": self.strategy,
            "resolved_content_hash": resolved_content_hash,
            "actions": [action.to_dict() for action in self.actions],
            "target_flow_ids": [flow.pk for flow in self.target_flows],
        }
