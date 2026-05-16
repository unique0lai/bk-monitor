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
    CONDITION_UNKNOWN,
    CONDITION_RECONCILED,
    CONDITION_TRUE,
    RecordRuleV4,
    RecordRuleV4Deployment,
    RecordRuleV4Event,
    RecordRuleV4Flow,
    RecordRuleV4Resolved,
    RecordRuleV4Spec,
    now,
    stable_hash,
)

logger = logging.getLogger("metadata")


class DeploymentRunner:
    """生成部署计划，并执行 plan 中的 Flow create/update/delete 动作。

    Runner 只处理物理部署层：根据 resolved 产出目标 Flow、对比已落地
    Flow、持久化 Deployment plan，并把 plan 里的动作下发给 bkbase。
    """

    def __init__(self, rule: RecordRuleV4, source: str = "system", operator: str = "") -> None:
        self.rule = rule
        self.source = source
        self.operator = operator

    @property
    def actor(self) -> str:
        return self.operator or self.source

    def reload_rule(self, for_update: bool = False) -> RecordRuleV4:
        """重新加载 rule，避免长流程中沿用旧指针。"""

        queryset = RecordRuleV4.objects
        if for_update:
            queryset = queryset.select_for_update()
        self.rule = queryset.get(pk=self.rule.pk)
        return self.rule

    def plan_for_spec(
        self, *, spec: RecordRuleV4Spec | None = None, resolved: RecordRuleV4Resolved | None = None
    ) -> RecordRuleV4Deployment | None:
        """为指定 spec/resolved 生成部署批次。

        如果目标 Flow 与当前 applied Flow 没有差异，则不会生成新的
        Deployment；这能避免 Flow 模板或运行态字段造成无意义刷新。
        """

        spec = spec or self.rule.current_spec
        if spec is None:
            return None
        resolved = resolved or self.rule.latest_resolved
        if resolved is None:
            return None

        plan = self.build_plan(spec=spec, resolved=resolved)
        if not plan.actions:
            # 没有动作时沿用已有 deployment 指针，维持声明式 API 的当前态。
            deployment = self.rule.applied_deployment or self.rule.latest_deployment
            if deployment:
                self.rule.use_deployment(deployment)
            return deployment

        plan_config = plan.to_config(
            desired_status=self.rule.desired_status, resolved_content_hash=resolved.content_hash
        )
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
            strategy=spec.deployment_strategy_name,
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
        """基于目标 Flow 和已成功落地 Flow 计算增删改动作。"""

        # 必须先取 applied 快照，再持久化 target flow。启停等运行态可能会
        # 更新同一 resolved 下的 Flow 配置，顺序反了会把旧配置覆盖掉。
        applied_flows = self.get_applied_flows()
        target_flows = (
            []
            if spec.desired_status == RecordRuleV4DesiredStatus.DELETED.value
            else self.persist_target_flows(spec, resolved)
        )
        applied_by_key = {flow.flow_key: flow for flow in applied_flows}
        target_by_key = {flow.flow_key: flow for flow in target_flows}

        actions: list[DeploymentPlanAction] = []
        # 目标集合里新增的 flow_key 是 create；同 key 但内容指纹不同是 update。
        for flow in target_flows:
            applied_flow = applied_by_key.get(flow.flow_key)
            if applied_flow is None:
                actions.append(self.build_action(RecordRuleV4FlowActionType.CREATE.value, flow))
            elif applied_flow.content_hash != flow.content_hash:
                actions.append(self.build_action(RecordRuleV4FlowActionType.UPDATE.value, flow))

        # applied 里仍存在、目标集合里不存在的 Flow 需要删除。
        for flow in applied_flows:
            if flow.flow_key not in target_by_key:
                actions.append(self.build_action(RecordRuleV4FlowActionType.DELETE.value, flow))

        return DeploymentPlan(
            strategy=copy.deepcopy(spec.deployment_strategy), actions=actions, target_flows=target_flows
        )

    def persist_target_flows(self, spec: RecordRuleV4Spec, resolved: RecordRuleV4Resolved) -> list[RecordRuleV4Flow]:
        """按部署策略生成并持久化目标 Flow 实体。"""

        strategy = get_deployment_strategy(spec.deployment_strategy_name)
        flow_plans = strategy.build_flows(rule=self.rule, spec=spec, resolved=resolved)
        flows: list[RecordRuleV4Flow] = []
        for flow_plan in flow_plans:
            flow = self.persist_flow(spec=spec, resolved=resolved, flow_plan=flow_plan)
            flows.append(flow)
        return flows

    def persist_flow(
        self, *, spec: RecordRuleV4Spec, resolved: RecordRuleV4Resolved, flow_plan: FlowPlan
    ) -> RecordRuleV4Flow:
        """保存单个目标 Flow，并维护 Flow 到 resolved record 的归属关系。"""

        flow, _ = RecordRuleV4Flow.objects.update_or_create(
            resolved=resolved,
            flow_key=flow_plan.flow_key,
            defaults={
                "rule": self.rule,
                "flow_name": flow_plan.flow_name,
                "strategy": spec.deployment_strategy_name,
                "table_id": self.rule.table_id,
                "dst_vm_table_id": self.rule.dst_vm_table_id,
                "flow_config": self.with_desired_status(flow_plan.flow_config, self.rule.desired_status),
                "content_hash": flow_plan.content_hash,
                "desired_status": self.rule.desired_status,
                "creator": self.actor,
                "updater": self.actor,
            },
        )
        for record in flow_plan.resolved_records:
            # 同一 resolved record 只能归属一个 Flow。策略或分组逻辑变化时，
            # 直接改写 record.flow 即可，不需要额外的关系实体。
            record.flow = flow
            record.updater = self.actor
            record.save(update_fields=["flow", "updater", "updated_at"])
        return flow

    def get_applied_flows(self) -> list[RecordRuleV4Flow]:
        """返回最近成功下发 deployment 对应的 Flow 快照。"""

        if not self.rule.applied_deployment_id:
            return []
        return list(self.rule.applied_deployment.resolved.flows.all())

    @staticmethod
    def build_action(action_type: str, flow: RecordRuleV4Flow) -> DeploymentPlanAction:
        """构造可重试的单个 Flow 动作。"""

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
        """获取同一 resolved 下的下一个部署版本号。"""

        latest = resolved.deployments.order_by("-deployment_version").first()
        return 1 if latest is None else latest.deployment_version + 1

    def apply(self, deployment: RecordRuleV4Deployment | None = None) -> bool:
        """执行 deployment plan 中尚未成功的动作。

        成功事件作为重试依据：如果上一次部分失败，下一次只执行未成功的
        action_key，避免把已成功的 Flow 全量重跑。
        """

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
        current_flow: RecordRuleV4Flow | None = None
        try:
            actions = deployment.plan_config.get("actions") or []
            for action in actions:
                if action["action_key"] in succeeded_action_keys:
                    continue
                current_flow = RecordRuleV4Flow.objects.get(pk=action["flow_id"])
                self.execute_action(deployment, current_flow, action)
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
                flow=current_flow,
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
        """执行一个 Flow action，并为开始 / 成功 / 失败写入事件。"""

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
                # plan 快照里保存的是计算定义，运行态 desired_status 以 rule
                # 当前值为准，避免启停污染 plan 内容指纹。
                flow_config = self.with_desired_status(action["flow_config_snapshot"], self.rule.desired_status)
                self.apply_flow(flow_config)
                flow.desired_status = self.rule.desired_status
                flow.flow_config = flow_config
                flow.save(update_fields=["desired_status", "flow_config", "updated_at"])
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

    def apply_desired_status(self, desired_status: str) -> bool:
        """直接下发 running/stopped 运行态，不生成新的 deployment plan。"""

        self.reload_rule()
        deployment = self.rule.applied_deployment
        if deployment is None:
            self.rule.sync_phase()
            self.rule.save(update_fields=["status", "updated_at"])
            return True

        current_flow: RecordRuleV4Flow | None = None
        try:
            for flow in deployment.resolved.flows.all():
                # 启停只改变 Flow spec.desired_status，其余配置保持 applied
                # 快照，避免触发新的 resolved / plan。
                current_flow = flow
                flow_config = self.with_desired_status(flow.flow_config, desired_status)
                self.apply_flow(flow_config)
                flow.desired_status = desired_status
                flow.flow_config = flow_config
                flow.save(update_fields=["desired_status", "flow_config", "updated_at"])
        except Exception as err:
            self.rule.last_error = str(err)
            self.rule.set_condition(CONDITION_RECONCILED, CONDITION_FALSE, "DesiredStatusApplyFailed", str(err))
            self.rule.sync_phase()
            self.rule.save(update_fields=["last_error", "conditions", "status", "updated_at"])
            RecordRuleV4Event.record_apply_failed(
                self.rule,
                deployment,
                source=self.source,
                operator=self.operator,
                message=str(err),
                flow=current_flow,
            )
            logger.exception("RecordRuleV4 apply desired status failed, id: %s", self.rule.pk)
            return False

        self.rule.last_error = ""
        self.rule.set_condition(CONDITION_RECONCILED, CONDITION_TRUE, "DesiredStatusApplied")
        self.rule.set_condition(CONDITION_FLOW_HEALTHY, CONDITION_UNKNOWN, "ApplySubmitted")
        self.rule.sync_phase()
        self.rule.save(update_fields=["last_error", "conditions", "status", "updated_at"])
        return True

    @staticmethod
    def with_desired_status(flow_config: dict[str, Any], desired_status: str) -> dict[str, Any]:
        """给 Flow 配置注入运行态 desired_status，并保持原配置不可变。"""

        next_config = copy.deepcopy(flow_config)
        next_config.setdefault("spec", {})["desired_status"] = desired_status
        return next_config

    @staticmethod
    def get_succeeded_action_keys(deployment: RecordRuleV4Deployment) -> set[str]:
        """从事件流中提取已成功动作，用于部分失败后的选择性重试。"""

        events = RecordRuleV4Event.objects.filter(
            deployment=deployment,
            event_type="flow_action.succeeded",
            status="succeeded",
        )
        return {event.detail.get("action_key") for event in events if event.detail.get("action_key")}

    def is_deployment_current(self, deployment: RecordRuleV4Deployment) -> bool:
        """确认待下发 deployment 仍是当前声明对应的 latest deployment。"""

        self.reload_rule()
        return (
            self.rule.latest_deployment_id == deployment.pk
            and self.rule.current_spec_id == deployment.spec_id
            and self.rule.generation == deployment.generation
        )

    def apply_flow(self, flow_config: dict[str, Any]) -> Any:
        """调用 bkbase v4 apply 接口创建或更新 Flow。"""

        response = api.bkdata.apply_data_link(bk_tenant_id=self.rule.bk_tenant_id, config=[flow_config])
        self.rule.last_refresh_time = now()
        return response

    def delete_flow(self, flow_name: str, ignore_not_found: bool = False) -> Any:
        """调用 bkbase 删除 Flow；可把 Not Found 视为删除成功。"""

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
        """观测 applied deployment 下所有 Flow 的实际状态并汇总到 group。"""

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
        """将多个 Flow 观测状态聚合成 group 级状态。"""

        if not statuses:
            return RecordRuleV4FlowStatus.NOT_FOUND.value
        if all(status == RecordRuleV4FlowStatus.OK.value for status in statuses):
            return RecordRuleV4FlowStatus.OK.value
        if any(status == RecordRuleV4FlowStatus.NOT_FOUND.value for status in statuses):
            return RecordRuleV4FlowStatus.NOT_FOUND.value
        return RecordRuleV4FlowStatus.ABNORMAL.value

    @staticmethod
    def is_not_found_error(err: Exception) -> bool:
        """粗略识别 bkbase Not Found 类错误。"""

        message = str(err).lower()
        return "not found" in message or "404" in message

    @staticmethod
    def extract_flow_status(flow_info: dict[str, Any]) -> str:
        """把 bkbase 返回的多种状态结构归一成 ok / abnormal。"""

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
