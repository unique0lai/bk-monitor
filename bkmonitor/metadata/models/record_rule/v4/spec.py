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

from django.db import transaction

from metadata.models.record_rule.v4.models import (
    RecordRuleV4,
    RecordRuleV4Spec,
    RecordRuleV4SpecRecord,
    generate_record_key,
    normalize_deployment_strategy,
    normalize_labels,
    stable_hash,
)
from metadata.models.record_rule.v4.types import RecordRuleV4RecordInput

logger = logging.getLogger("metadata")


class RecordRuleV4SpecBuilder:
    """负责创建用户声明快照，以及为组内 record 分配稳定 key。

    SpecBuilder 只处理用户输入层，不调用 unify-query，也不生成 Flow。
    """

    def __init__(self, rule: RecordRuleV4, source: str = "system", operator: str = "") -> None:
        self.rule = rule
        self.source = source
        self.operator = operator

    @property
    def actor(self) -> str:
        return self.operator or self.source

    def create_spec(
        self,
        *,
        records: list[RecordRuleV4RecordInput],
        raw_config: dict[str, Any],
        interval: str,
        deployment_strategy: str | dict[str, Any] | None,
        desired_status: str,
        labels: list[dict[str, Any]] | None = None,
    ) -> RecordRuleV4Spec:
        """创建一份新的 spec 快照和对应的 spec records。"""

        RecordRuleV4.validate_desired_status(desired_status)
        RecordRuleV4.validate_interval(interval)
        group_labels = normalize_labels(labels)
        deployment_strategy_config = normalize_deployment_strategy(deployment_strategy)
        normalized_records = [self.normalize_record_payload(record) for record in records]
        generation = self.rule.generation + 1
        # spec content_hash 表达用户声明内容；resolved 漂移和 Flow 模板变化
        # 都不应该混入这一层。
        content_payload = {
            "records": [self.record_content_payload(record) for record in normalized_records],
            "raw_config": raw_config,
            "interval": interval,
            "labels": group_labels,
            "deployment_strategy": deployment_strategy_config,
            "desired_status": desired_status,
        }

        with transaction.atomic():
            spec = RecordRuleV4Spec.objects.create(
                rule=self.rule,
                generation=generation,
                raw_config=copy.deepcopy(raw_config),
                interval=interval,
                labels=copy.deepcopy(group_labels),
                deployment_strategy=copy.deepcopy(deployment_strategy_config),
                desired_status=desired_status,
                content_hash=stable_hash(content_payload),
                source=self.source,
                operator=self.operator,
                creator=self.actor,
                updater=self.actor,
            )
            for source_index, record in enumerate(self.assign_record_keys(normalized_records)):
                RecordRuleV4SpecRecord.objects.create(
                    spec=spec,
                    source_index=source_index,
                    record_key=record["record_key"],
                    identity_hash=record["identity_hash"],
                    content_hash=record["content_hash"],
                    record_name=record["record_name"],
                    input_type=record["input_type"],
                    input_config=record["input_config"],
                    metric_name=record["metric_name"],
                    labels=record["labels"],
                    creator=self.actor,
                    updater=self.actor,
                )
        return spec

    def normalize_record_payload(self, record: RecordRuleV4RecordInput) -> dict[str, Any]:
        """归一化单条用户 record，并校验输入类型。"""

        normalized = RecordRuleV4SpecRecord.normalize_record_payload(copy.deepcopy(record))
        RecordRuleV4.validate_input_type(normalized["input_type"])
        return normalized

    @staticmethod
    def record_content_payload(record: dict[str, Any]) -> dict[str, Any]:
        """返回参与单条 record 内容指纹计算的字段。"""

        return {
            "record_name": record["record_name"],
            "input_type": record["input_type"],
            "input_config": record["input_config"],
            "metric_name": record["metric_name"],
            "labels": record["labels"],
        }

    def assign_record_keys(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """为 records 分配稳定 record_key。

        API 模式可以显式传 record_key；SCode 等隐藏 key 的模式则按
        identity_hash 继承上一版 record_key，避免轻微内容修改导致 Flow
        身份整体变化。
        """

        previous_records = []
        if self.rule.current_spec_id:
            previous_records = list(self.rule.current_spec.records.all())
        previous_by_key = {record.record_key: record for record in previous_records}
        previous_by_identity = {record.identity_hash: record for record in previous_records}

        seen_keys: set[str] = set()
        seen_identity: set[str] = set()
        result: list[dict[str, Any]] = []
        for record in records:
            identity_hash = stable_hash(RecordRuleV4SpecRecord.identity_payload(record))
            if identity_hash in seen_identity:
                raise ValueError(f"duplicate record identity in group: {record['record_name']}")
            seen_identity.add(identity_hash)

            explicit_key = record.get("record_key") or ""
            if explicit_key:
                record_key = explicit_key
            elif identity_hash in previous_by_identity:
                # 用户不传 key 时，稳定身份相同就继承旧 key。
                record_key = previous_by_identity[identity_hash].record_key
            else:
                record_key = generate_record_key()

            if record_key in seen_keys:
                raise ValueError(f"duplicate record_key in group: {record_key}")
            if (
                explicit_key
                and explicit_key in previous_by_key
                and previous_by_key[explicit_key].identity_hash != identity_hash
            ):
                logger.info(
                    "RecordRuleV4 spec record identity changed, rule_id: %s, record_key: %s",
                    self.rule.pk,
                    explicit_key,
                )
            seen_keys.add(record_key)

            next_record = dict(record)
            next_record["record_key"] = record_key
            next_record["identity_hash"] = identity_hash
            next_record["content_hash"] = stable_hash(self.record_content_payload(record))
            result.append(next_record)
        return result

    @staticmethod
    def dump_spec_records(spec: RecordRuleV4Spec) -> list[RecordRuleV4RecordInput]:
        """把已有 spec records 还原成 create_spec 可消费的输入结构。"""

        records: list[RecordRuleV4RecordInput] = []
        for record in spec.records.order_by("source_index", "id"):
            records.append(
                {
                    "record_key": record.record_key,
                    "record_name": record.record_name,
                    "input_type": record.input_type,
                    "input_config": copy.deepcopy(record.input_config),
                    "metric_name": record.metric_name,
                    "labels": copy.deepcopy(record.labels),
                }
            )
        return records
