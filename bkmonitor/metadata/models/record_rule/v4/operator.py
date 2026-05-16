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
from collections.abc import Callable
from typing import Any, TypeVar

from django.db import transaction

from bkmonitor.utils.tenant import space_uid_to_bk_tenant_id
from metadata.models.record_rule.constants import (
    RecordRuleV4DeploymentStrategy,
    RecordRuleV4DesiredStatus,
    RecordRuleV4FlowStatus,
)
from metadata.models.record_rule.v4.deployment.runner import DeploymentRunner
from metadata.models.record_rule.v4.models import (
    RecordRuleV4,
    RecordRuleV4Deployment,
    RecordRuleV4Event,
    RecordRuleV4Resolved,
    RecordRuleV4Spec,
)
from metadata.models.record_rule.v4.resolver import RecordRuleV4Resolver
from metadata.models.record_rule.v4.spec import RecordRuleV4SpecBuilder

T = TypeVar("T")


class RecordRuleV4Operator:
    """串联 V4 预计算 group 的声明态、解析态和部署态。"""

    def __init__(self, rule: RecordRuleV4, source: str = "system", operator: str = "") -> None:
        self.rule = rule
        self.source = source
        self.operator = operator

    @property
    def actor(self) -> str:
        return self.operator or self.source

    @property
    def spec_builder(self) -> RecordRuleV4SpecBuilder:
        return RecordRuleV4SpecBuilder(self.rule, source=self.source, operator=self.operator)

    @property
    def resolver(self) -> RecordRuleV4Resolver:
        return RecordRuleV4Resolver(self.rule, source=self.source, operator=self.operator)

    @property
    def deployment_runner(self) -> DeploymentRunner:
        return DeploymentRunner(self.rule, source=self.source, operator=self.operator)

    def reload_rule(self, for_update: bool = False) -> RecordRuleV4:
        queryset = RecordRuleV4.objects
        if for_update:
            queryset = queryset.select_for_update()
        self.rule = queryset.get(pk=self.rule.pk)
        return self.rule

    def require_current_spec(self) -> RecordRuleV4Spec:
        spec = self.rule.current_spec
        if spec is None:
            raise ValueError("current spec is missing")
        return spec

    def run_with_operation_lock(self, reason: str, callback: Callable[[], T], locked_result: T) -> T:
        token = self.rule.acquire_operation_lock(owner=self.actor, reason=reason)
        if not token:
            self.reload_rule()
            RecordRuleV4Event.record_operation_locked(
                self.rule, operation=reason, source=self.source, operator=self.operator
            )
            return locked_result

        try:
            self.reload_rule()
            return callback()
        finally:
            self.rule.release_operation_lock(token)

    @classmethod
    def create(
        cls,
        *,
        space_type: str,
        space_id: str,
        group_name: str,
        records: list[dict[str, Any]],
        raw_config: dict[str, Any] | None = None,
        bk_tenant_id: str | None = None,
        auto_refresh: bool = True,
        deployment_strategy: str = RecordRuleV4DeploymentStrategy.PER_RECORD.value,
        source: str = "user",
        operator: str = "",
        apply_immediately: bool = True,
    ) -> RecordRuleV4:
        RecordRuleV4.validate_deployment_strategy(deployment_strategy)
        bk_tenant_id = bk_tenant_id or space_uid_to_bk_tenant_id(f"{space_type}__{space_id}")
        table_id = RecordRuleV4.compose_table_id(group_name)

        rule = RecordRuleV4.objects.create(
            bk_tenant_id=bk_tenant_id,
            space_type=space_type,
            space_id=space_id,
            group_name=group_name,
            table_id=table_id,
            dst_vm_table_id=RecordRuleV4.compose_dst_vm_table_id(table_id),
            auto_refresh=auto_refresh,
            creator=operator or source,
            updater=operator or source,
        )
        instance = cls(rule, source=source, operator=operator)
        spec = instance.spec_builder.create_spec(
            records=records,
            raw_config=raw_config or {"records": records},
            desired_status=RecordRuleV4DesiredStatus.RUNNING.value,
            deployment_strategy=deployment_strategy,
        )
        rule.use_spec(spec)
        RecordRuleV4Event.record_user_create(rule, spec, source=source, operator=operator)

        resolved = instance.resolver.resolve_current(force=True)
        if resolved:
            instance.deployment_runner.plan_for_spec(spec=spec, resolved=resolved)
        if apply_immediately and resolved:
            instance.apply()
        instance.rule.refresh_from_db()
        return instance.rule

    def update_spec(
        self,
        *,
        records: list[dict[str, Any]] | object = RecordRuleV4.UNSET,
        raw_config: dict[str, Any] | object = RecordRuleV4.UNSET,
        desired_status: str | object = RecordRuleV4.UNSET,
        deployment_strategy: str | object = RecordRuleV4.UNSET,
        auto_refresh: bool | object = RecordRuleV4.UNSET,
        apply_immediately: bool = True,
    ) -> RecordRuleV4:
        spec: RecordRuleV4Spec | None = None
        records_changed = False
        definition_changed = False
        runtime_desired_status: str | None = None
        runtime_desired_status_changed = False

        with transaction.atomic():
            self.reload_rule(for_update=True)
            current_spec = self.require_current_spec()
            next_records: list[dict[str, Any]] = (
                RecordRuleV4SpecBuilder.dump_spec_records(current_spec)
                if records is RecordRuleV4.UNSET
                else list(records)
            )
            next_raw_config = current_spec.raw_config if raw_config is RecordRuleV4.UNSET else dict(raw_config)
            requested_desired_status = None if desired_status is RecordRuleV4.UNSET else str(desired_status)
            if requested_desired_status is not None:
                RecordRuleV4.validate_desired_status(requested_desired_status)
            runtime_desired_status = (
                requested_desired_status
                if requested_desired_status
                in {RecordRuleV4DesiredStatus.RUNNING.value, RecordRuleV4DesiredStatus.STOPPED.value}
                else None
            )
            next_desired_status = (
                RecordRuleV4DesiredStatus.DELETED.value
                if requested_desired_status == RecordRuleV4DesiredStatus.DELETED.value
                else current_spec.desired_status
            )
            next_strategy = (
                current_spec.deployment_strategy
                if deployment_strategy is RecordRuleV4.UNSET
                else str(deployment_strategy)
            )

            auto_refresh_changed = (
                auto_refresh is not RecordRuleV4.UNSET and bool(auto_refresh) != self.rule.auto_refresh
            )
            if auto_refresh_changed:
                self.rule.auto_refresh = bool(auto_refresh)

            records_changed = records is not RecordRuleV4.UNSET or raw_config is not RecordRuleV4.UNSET
            definition_changed = (
                next_desired_status != current_spec.desired_status or next_strategy != current_spec.deployment_strategy
            )
            runtime_desired_status_changed = (
                runtime_desired_status is not None and runtime_desired_status != self.rule.desired_status
            )
            if runtime_desired_status_changed and runtime_desired_status:
                self.rule.set_desired_status(runtime_desired_status)
                RecordRuleV4Event.record_user_desired_status_changed(
                    self.rule, source=self.source, operator=self.operator
                )

            changed_fields: list[str] = []
            if records_changed:
                changed_fields.append("records")
            if raw_config is not RecordRuleV4.UNSET:
                changed_fields.append("raw_config")
            if requested_desired_status == RecordRuleV4DesiredStatus.DELETED.value:
                changed_fields.append("desired_status")
            if next_strategy != current_spec.deployment_strategy:
                changed_fields.append("deployment_strategy")

            if not changed_fields:
                if auto_refresh_changed:
                    self.rule.sync_phase()
                    self.rule.save(update_fields=["auto_refresh", "status", "updated_at"])
                    RecordRuleV4Event.record_user_auto_refresh_changed(
                        self.rule, source=self.source, operator=self.operator
                    )
                if not runtime_desired_status_changed:
                    return self.rule
            else:
                spec = self.spec_builder.create_spec(
                    records=next_records,
                    raw_config=copy.deepcopy(next_raw_config),
                    desired_status=next_desired_status,
                    deployment_strategy=next_strategy,
                )
                self.rule.use_spec(spec)
                RecordRuleV4Event.record_user_spec_changed(
                    self.rule,
                    spec,
                    source=self.source,
                    operator=self.operator,
                    changed_fields=changed_fields,
                )

        if records_changed and spec.desired_status != RecordRuleV4DesiredStatus.DELETED.value:
            previous_resolved_id = self.rule.latest_resolved_id
            resolved = self.refresh_resolved(force=False)
            self.reload_rule()
            if resolved and resolved.pk != previous_resolved_id:
                self.deployment_runner.plan_for_spec(spec=spec, resolved=resolved)
        elif definition_changed:
            self.deployment_runner.plan_for_spec(spec=spec, resolved=self.rule.latest_resolved)

        self.reload_rule()
        if apply_immediately and self.rule.update_available:
            self.apply()
        elif apply_immediately and runtime_desired_status_changed and runtime_desired_status:
            self.deployment_runner.apply_desired_status(runtime_desired_status)
        self.reload_rule()
        return self.rule

    def delete(self, apply_immediately: bool = True) -> RecordRuleV4:
        return self.update_spec(
            desired_status=RecordRuleV4DesiredStatus.DELETED.value,
            apply_immediately=apply_immediately,
        )

    def manual_refresh(self) -> RecordRuleV4Resolved | None:
        return self.run_with_operation_lock(
            "manual_refresh",
            self.manual_refresh_unlocked,
            None,
        )

    def manual_refresh_unlocked(self) -> RecordRuleV4Resolved | None:
        previous_resolved_id = self.rule.latest_resolved_id
        resolved = self.refresh_resolved(force=False)
        if resolved and resolved.pk != previous_resolved_id:
            self.reload_rule()
            self.deployment_runner.plan_for_spec(spec=self.rule.current_spec, resolved=resolved)
        return resolved

    def reconcile(self, auto_apply: bool | None = None) -> bool:
        return self.run_with_operation_lock(
            "reconcile",
            lambda: self.reconcile_unlocked(auto_apply=auto_apply),
            False,
        )

    def reconcile_unlocked(self, auto_apply: bool | None = None) -> bool:
        previous_resolved_id = self.rule.latest_resolved_id
        resolved = self.refresh_resolved(force=False)
        changed = bool(resolved and resolved.pk != previous_resolved_id)
        if changed and resolved:
            self.reload_rule()
            self.deployment_runner.plan_for_spec(spec=self.rule.current_spec, resolved=resolved)
        self.reload_rule()
        should_apply = self.rule.auto_refresh if auto_apply is None else auto_apply
        if should_apply and self.rule.update_available and self.rule.latest_deployment_id:
            self.deployment_runner.apply(self.rule.latest_deployment)
        return changed

    def refresh_resolved(self, force: bool = False) -> RecordRuleV4Resolved | None:
        return self.resolver.resolve_current(force=force)

    def apply(self, deployment: RecordRuleV4Deployment | None = None) -> bool:
        return self.run_with_operation_lock(
            "apply",
            lambda: self.deployment_runner.apply(deployment=deployment),
            False,
        )

    def refresh_flow_health(self) -> str:
        return self.run_with_operation_lock(
            "refresh_flow_health",
            self.deployment_runner.refresh_flow_health,
            RecordRuleV4FlowStatus.ABNORMAL.value,
        )
