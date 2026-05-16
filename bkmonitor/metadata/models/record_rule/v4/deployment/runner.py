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

from core.drf_resource import api
from metadata.models.record_rule.constants import (
    RECORD_RULE_V4_BKBASE_NAMESPACE,
    RecordRuleV4DesiredStatus,
    RecordRuleV4FlowActionType,
    RecordRuleV4FlowStatus,
)
from metadata.models.record_rule.v4.deployment.plan import DeploymentPlan, DeploymentPlanAction, FlowPlan
from metadata.models.record_rule.v4.deployment.strategy import get_deployment_strategy
from metadata.models.record_rule.v4.models import (
    CONDITION_FALSE,
    CONDITION_FLOW_HEALTHY,
    CONDITION_RECONCILED,
    CONDITION_TRUE,
    RecordRuleV4,
    RecordRuleV4Deployment,
    RecordRuleV4Event,
    RecordRuleV4Flow,
    RecordRuleV4FlowRecord,
    RecordRuleV4Resolved,
    RecordRuleV4Spec,
    now,
    stable_hash,
)

logger = logging.getLogger("metadata")


class DeploymentRunner:
    """生成部署计划，并执行 plan 中的 Flow create/update/delete 动作。"""

    def __init__(self, rule: RecordRuleV4, source: str = "system", operator: str = "") -> None:
        self.rule = rule
        self.source = source
        self.operator = operator

    @property
    def actor(self) -> str:
        return self.operator or self.source

    def reload_rule(self, for_update: bool = False) -> RecordRuleV4:
        queryset = RecordRuleV4.objects
        if for_update:
            queryset = queryset.select_for_update()
        self.rule = queryset.get(pk=self.rule.pk)
        return self.rule

    def plan_for_spec(
        self, *, spec: RecordRuleV4Spec | None = None, resolved: RecordRuleV4Resolved | None = None
    ) -> RecordRuleV4Deployment | None:
        spec = spec or self.rule.current_spec
        if spec is None:
            return None
        resolved = resolved or self.rule.latest_resolved
        if resolved is None:
            return None

        plan = self.build_plan(spec=spec, resolved=resolved)
        if not plan.actions:
            deployment = self.rule.applied_deployment or self.rule.latest_deployment
            if deployment:
                self.rule.use_deployment(deployment)
            return deployment

        plan_config = plan.to_config(desired_status=spec.desired_status, resolved_content_hash=resolved.content_hash)
        content_hash = stable_hash(plan_config)
        latest_deployment = self.rule.latest_deployment
        if latest_deployment and latest_deployment.content_hash == content_hash:
            RecordRuleV4Event.record_deployment_planned(
                self.rule,
                latest_deployment,
                source=self.source,
                operator=self.operator,
                unchanged=True,
            )
            self.rule.use_deployment(latest_deployment)
            return latest_deployment

        deployment = RecordRuleV4Deployment.objects.create(
            rule=self.rule,
            spec=spec,
            resolved=resolved,
            generation=spec.generation,
            deployment_version=self.next_deployment_version(resolved),
            strategy=spec.deployment_strategy,
            content_hash=content_hash,
            plan_config=plan_config,
            source=self.source,
            creator=self.actor,
            updater=self.actor,
        )
        RecordRuleV4Event.record_deployment_planned(
            self.rule,
            deployment,
            source=self.source,
            operator=self.operator,
        )
        self.rule.use_deployment(deployment)
        return deployment

    def build_plan(self, *, spec: RecordRuleV4Spec, resolved: RecordRuleV4Resolved) -> DeploymentPlan:
        applied_flows = self.get_applied_flows()
        target_flows = (
            []
            if spec.desired_status == RecordRuleV4DesiredStatus.DELETED.value
            else self.persist_target_flows(spec, resolved)
        )
        applied_by_key = {flow.flow_key: flow for flow in applied_flows}
        target_by_key = {flow.flow_key: flow for flow in target_flows}

        actions: list[DeploymentPlanAction] = []
        for flow in target_flows:
            applied_flow = applied_by_key.get(flow.flow_key)
            if applied_flow is None:
                actions.append(self.build_action(RecordRuleV4FlowActionType.CREATE.value, flow))
            elif applied_flow.content_hash != flow.content_hash:
                actions.append(self.build_action(RecordRuleV4FlowActionType.UPDATE.value, flow))

        for flow in applied_flows:
            if flow.flow_key not in target_by_key:
                actions.append(self.build_action(RecordRuleV4FlowActionType.DELETE.value, flow))

        return DeploymentPlan(strategy=spec.deployment_strategy, actions=actions, target_flows=target_flows)

    def persist_target_flows(self, spec: RecordRuleV4Spec, resolved: RecordRuleV4Resolved) -> list[RecordRuleV4Flow]:
        strategy = get_deployment_strategy(spec.deployment_strategy)
        flow_plans = strategy.build_flows(rule=self.rule, spec=spec, resolved=resolved)
        flows: list[RecordRuleV4Flow] = []
        for flow_plan in flow_plans:
            flow = self.persist_flow(spec=spec, resolved=resolved, flow_plan=flow_plan)
            flows.append(flow)
        return flows

    def persist_flow(
        self, *, spec: RecordRuleV4Spec, resolved: RecordRuleV4Resolved, flow_plan: FlowPlan
    ) -> RecordRuleV4Flow:
        flow, _ = RecordRuleV4Flow.objects.update_or_create(
            resolved=resolved,
            flow_key=flow_plan.flow_key,
            defaults={
                "rule": self.rule,
                "flow_name": flow_plan.flow_name,
                "strategy": spec.deployment_strategy,
                "table_id": self.rule.table_id,
                "dst_vm_table_id": self.rule.dst_vm_table_id,
                "flow_config": flow_plan.flow_config,
                "content_hash": flow_plan.content_hash,
                "desired_status": spec.desired_status,
                "creator": self.actor,
                "updater": self.actor,
            },
        )
        for record in flow_plan.resolved_records:
            RecordRuleV4FlowRecord.objects.update_or_create(
                resolved_record=record,
                defaults={"flow": flow, "creator": self.actor, "updater": self.actor},
            )
        return flow

    def get_applied_flows(self) -> list[RecordRuleV4Flow]:
        if not self.rule.applied_deployment_id:
            return []
        return list(self.rule.applied_deployment.resolved.flows.all())

    @staticmethod
    def build_action(action_type: str, flow: RecordRuleV4Flow) -> DeploymentPlanAction:
        content_hash = stable_hash(
            {
                "action_type": action_type,
                "flow_key": flow.flow_key,
                "flow_name": flow.flow_name,
                "flow_hash": flow.content_hash,
            }
        )
        return DeploymentPlanAction(
            action_key=f"{action_type}:{flow.flow_key}:{flow.content_hash[:12]}",
            action_type=action_type,
            flow=flow,
            content_hash=content_hash,
            flow_config_snapshot=copy.deepcopy(flow.flow_config),
        )

    @staticmethod
    def next_deployment_version(resolved: RecordRuleV4Resolved) -> int:
        latest = resolved.deployments.order_by("-deployment_version").first()
        return 1 if latest is None else latest.deployment_version + 1

    def apply(self, deployment: RecordRuleV4Deployment | None = None) -> bool:
        self.reload_rule()
        deployment = deployment or self.rule.latest_deployment
        if deployment is None:
            self.rule.set_condition(CONDITION_RECONCILED, CONDITION_FALSE, "DeploymentMissing")
            self.rule.sync_phase()
            self.rule.save()
            RecordRuleV4Event.record_apply_failed_missing_deployment(
                self.rule, source=self.source, operator=self.operator
            )
            return False
        if not self.is_deployment_current(deployment):
            RecordRuleV4Event.record_apply_skipped_stale_deployment(
                self.rule, deployment, source=self.source, operator=self.operator
            )
            return False

        RecordRuleV4Event.record_apply_started(self.rule, deployment, source=self.source, operator=self.operator)
        succeeded_action_keys = self.get_succeeded_action_keys(deployment)
        try:
            actions = deployment.plan_config.get("actions") or []
            if any(action["action_type"] != RecordRuleV4FlowActionType.DELETE.value for action in actions):
                self.ensure_output_result_table(deployment)
            for action in actions:
                if action["action_key"] in succeeded_action_keys:
                    continue
                flow = RecordRuleV4Flow.objects.get(pk=action["flow_id"])
                self.execute_action(deployment, flow, action)
        except Exception as err:
            deployment.mark_apply_failed(err)
            is_current = self.is_deployment_current(deployment)
            if is_current:
                self.rule.last_error = str(err)
                self.rule.update_available = True
                self.rule.set_condition(CONDITION_RECONCILED, CONDITION_FALSE, "ApplyFailed", str(err))
                self.rule.sync_phase()
                self.rule.save()
            RecordRuleV4Event.record_apply_failed(
                self.rule,
                deployment,
                source=self.source,
                operator=self.operator,
                message=str(err),
                flow=flow if "flow" in locals() else None,
                stale=not is_current,
            )
            logger.exception(
                "RecordRuleV4 apply deployment failed, id: %s, deployment_id: %s", self.rule.pk, deployment.pk
            )
            return False

        deployment.mark_apply_succeeded()
        if not self.is_deployment_current(deployment):
            RecordRuleV4Event.record_apply_skipped_stale_deployment(
                self.rule, deployment, source=self.source, operator=self.operator
            )
            return False

        self.rule.mark_deployment_applied(deployment)
        RecordRuleV4Event.record_apply_succeeded(self.rule, deployment, source=self.source, operator=self.operator)
        return True

    def execute_action(
        self, deployment: RecordRuleV4Deployment, flow: RecordRuleV4Flow, action: dict[str, Any]
    ) -> None:
        action_key = action["action_key"]
        action_type = action["action_type"]
        RecordRuleV4Event.record_flow_action_started(
            self.rule,
            deployment,
            flow,
            action_key=action_key,
            action_type=action_type,
            source=self.source,
            operator=self.operator,
        )
        try:
            if action_type == RecordRuleV4FlowActionType.DELETE.value:
                self.delete_flow(flow.flow_name, ignore_not_found=True)
            else:
                self.apply_flow(action["flow_config_snapshot"])
        except Exception as err:
            RecordRuleV4Event.record_flow_action_result(
                self.rule,
                deployment,
                flow,
                action_key=action_key,
                action_type=action_type,
                succeeded=False,
                source=self.source,
                operator=self.operator,
                message=str(err),
            )
            raise
        RecordRuleV4Event.record_flow_action_result(
            self.rule,
            deployment,
            flow,
            action_key=action_key,
            action_type=action_type,
            succeeded=True,
            source=self.source,
            operator=self.operator,
        )

    @staticmethod
    def get_succeeded_action_keys(deployment: RecordRuleV4Deployment) -> set[str]:
        events = RecordRuleV4Event.objects.filter(
            deployment=deployment,
            event_type="flow_action.succeeded",
            status="succeeded",
        )
        return {event.detail.get("action_key") for event in events if event.detail.get("action_key")}

    def is_deployment_current(self, deployment: RecordRuleV4Deployment) -> bool:
        self.reload_rule()
        return (
            self.rule.latest_deployment_id == deployment.pk
            and self.rule.current_spec_id == deployment.spec_id
            and self.rule.generation == deployment.generation
        )

    def ensure_output_result_table(self, deployment: RecordRuleV4Deployment) -> None:
        from metadata import models as metadata_models

        biz_id = metadata_models.Space.objects.get_biz_id_by_space(self.rule.space_type, self.rule.space_id)
        metadata_models.ResultTable.objects.get_or_create(
            bk_tenant_id=self.rule.bk_tenant_id,
            table_id=self.rule.table_id,
            defaults={
                "table_name_zh": self.rule.table_id,
                "is_custom_table": True,
                "default_storage": metadata_models.ClusterInfo.TYPE_VM,
                "creator": "system",
                "bk_biz_id": biz_id,
            },
        )
        for metric_name in self.extract_output_metric_names_from_deployment(deployment):
            metadata_models.ResultTableField.objects.get_or_create(
                bk_tenant_id=self.rule.bk_tenant_id,
                table_id=self.rule.table_id,
                field_name=metric_name,
                defaults={
                    "field_type": metadata_models.ResultTableField.FIELD_TYPE_FLOAT,
                    "description": metric_name,
                    "tag": metadata_models.ResultTableField.FIELD_TAG_METRIC,
                    "is_config_by_user": True,
                },
            )

        vm_cluster_id = self.extract_vm_cluster_id(deployment)
        metadata_models.AccessVMRecord.objects.get_or_create(
            bk_tenant_id=self.rule.bk_tenant_id,
            result_table_id=self.rule.table_id,
            defaults={
                "bk_base_data_id": 0,
                "vm_result_table_id": self.rule.dst_vm_table_id,
                "vm_cluster_id": vm_cluster_id,
            },
        )

    @staticmethod
    def extract_output_metric_names_from_deployment(deployment: RecordRuleV4Deployment) -> list[str]:
        names: list[str] = []
        flow_ids = [action["flow_id"] for action in deployment.plan_config.get("actions") or []]
        for flow in RecordRuleV4Flow.objects.filter(pk__in=flow_ids):
            for node in flow.flow_config.get("spec", {}).get("nodes", []):
                if node.get("kind") != "RecordingRuleNode":
                    continue
                for item in node.get("config") or []:
                    metric_name = item.get("metric_name")
                    if metric_name and metric_name not in names:
                        names.append(metric_name)
        return names

    @staticmethod
    def extract_vm_cluster_id(deployment: RecordRuleV4Deployment) -> int | None:
        record = deployment.resolved.records.exclude(vm_cluster_id__isnull=True).first()
        return record.vm_cluster_id if record else None

    def apply_flow(self, flow_config: dict[str, Any]) -> Any:
        response = api.bkdata.apply_data_link(bk_tenant_id=self.rule.bk_tenant_id, config=[flow_config])
        self.rule.last_refresh_time = now()
        return response

    def delete_flow(self, flow_name: str, ignore_not_found: bool = False) -> Any:
        try:
            return api.bkdata.delete_data_link(
                bk_tenant_id=self.rule.bk_tenant_id,
                namespace=RECORD_RULE_V4_BKBASE_NAMESPACE,
                kind="flows",
                name=flow_name,
            )
        except Exception as err:
            if ignore_not_found and self.is_not_found_error(err):
                return {"status": RecordRuleV4FlowStatus.NOT_FOUND.value}
            raise

    def refresh_flow_health(self) -> str:
        self.reload_rule()
        deployment = self.rule.applied_deployment
        if deployment is None:
            self.rule.set_condition(CONDITION_FLOW_HEALTHY, CONDITION_FALSE, "DeploymentMissing")
            self.rule.sync_phase()
            self.rule.save()
            return RecordRuleV4FlowStatus.ABNORMAL.value

        observed_statuses: list[str] = []
        for flow in deployment.resolved.flows.all():
            try:
                flow_info = api.bkdata.get_data_link(
                    bk_tenant_id=self.rule.bk_tenant_id,
                    namespace=RECORD_RULE_V4_BKBASE_NAMESPACE,
                    kind="flows",
                    name=flow.flow_name,
                )
                status = self.extract_flow_status(flow_info or {})
                message = ""
                observe_succeeded = status == RecordRuleV4FlowStatus.OK.value
            except Exception as err:
                status = (
                    RecordRuleV4FlowStatus.NOT_FOUND.value
                    if self.is_not_found_error(err)
                    else RecordRuleV4FlowStatus.ABNORMAL.value
                )
                message = str(err)
                observe_succeeded = False

            flow.mark_flow_observed(status)
            observed_statuses.append(status)
            RecordRuleV4Event.record_flow_observed(
                self.rule,
                status,
                source=self.source,
                operator=self.operator,
                flow=flow,
                message=message,
                observe_succeeded=observe_succeeded,
            )

        aggregate = self.aggregate_flow_status(observed_statuses)
        condition_status = CONDITION_TRUE if aggregate == RecordRuleV4FlowStatus.OK.value else CONDITION_FALSE
        self.rule.set_condition(CONDITION_FLOW_HEALTHY, condition_status, aggregate)
        self.rule.sync_phase()
        self.rule.save()
        return aggregate

    @staticmethod
    def aggregate_flow_status(statuses: list[str]) -> str:
        if not statuses:
            return RecordRuleV4FlowStatus.NOT_FOUND.value
        if all(status == RecordRuleV4FlowStatus.OK.value for status in statuses):
            return RecordRuleV4FlowStatus.OK.value
        if any(status == RecordRuleV4FlowStatus.NOT_FOUND.value for status in statuses):
            return RecordRuleV4FlowStatus.NOT_FOUND.value
        return RecordRuleV4FlowStatus.ABNORMAL.value

    @staticmethod
    def is_not_found_error(err: Exception) -> bool:
        message = str(err).lower()
        return "not found" in message or "404" in message

    @staticmethod
    def extract_flow_status(flow_info: dict[str, Any]) -> str:
        status_info = flow_info.get("status") or {}
        if isinstance(status_info, str):
            return (
                RecordRuleV4FlowStatus.OK.value
                if status_info.lower() == "ok"
                else RecordRuleV4FlowStatus.ABNORMAL.value
            )
        if isinstance(status_info, dict):
            for key in ["status", "phase", "state"]:
                value = status_info.get(key)
                if value:
                    return (
                        RecordRuleV4FlowStatus.OK.value
                        if str(value).lower() == "ok"
                        else RecordRuleV4FlowStatus.ABNORMAL.value
                    )
        return RecordRuleV4FlowStatus.ABNORMAL.value
