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
    stable_hash,
)

logger = logging.getLogger("metadata")


class RecordRuleV4SpecBuilder:
    """负责创建用户声明快照，以及为组内 record 分配稳定 key。"""

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
        records: list[dict[str, Any]],
        raw_config: dict[str, Any],
        desired_status: str,
        deployment_strategy: str,
    ) -> RecordRuleV4Spec:
        RecordRuleV4.validate_desired_status(desired_status)
        RecordRuleV4.validate_deployment_strategy(deployment_strategy)
        normalized_records = [self.normalize_record_payload(record) for record in records]
        generation = self.rule.generation + 1
        content_payload = {
            "records": [self.record_content_payload(record) for record in normalized_records],
            "raw_config": raw_config,
            "desired_status": desired_status,
            "deployment_strategy": deployment_strategy,
        }

        with transaction.atomic():
            spec = RecordRuleV4Spec.objects.create(
                rule=self.rule,
                generation=generation,
                raw_config=copy.deepcopy(raw_config),
                desired_status=desired_status,
                deployment_strategy=deployment_strategy,
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
                    interval=record["interval"],
                    creator=self.actor,
                    updater=self.actor,
                )
        return spec

    def normalize_record_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        normalized = RecordRuleV4SpecRecord.normalize_record_payload(copy.deepcopy(record))
        RecordRuleV4.validate_input_type(normalized["input_type"])
        RecordRuleV4.validate_interval(normalized["interval"])
        return normalized

    @staticmethod
    def record_content_payload(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "record_name": record["record_name"],
            "input_type": record["input_type"],
            "input_config": record["input_config"],
            "metric_name": record["metric_name"],
            "labels": record["labels"],
            "interval": record["interval"],
        }

    def assign_record_keys(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    def dump_spec_records(spec: RecordRuleV4Spec) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for record in spec.records.order_by("source_index", "id"):
            records.append(
                {
                    "record_key": record.record_key,
                    "record_name": record.record_name,
                    "input_type": record.input_type,
                    "input_config": copy.deepcopy(record.input_config),
                    "metric_name": record.metric_name,
                    "labels": copy.deepcopy(record.labels),
                    "interval": record.interval,
                }
            )
        return records
