"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

from types import SimpleNamespace

import pytest

from metadata import models
from metadata.models.record_rule.constants import (
    RECORD_RULE_V4_BKBASE_NAMESPACE,
    RECORD_RULE_V4_BKMONITOR_NAMESPACE,
    RECORD_RULE_V4_DEFAULT_TENANT,
    RecordRuleV4ApplyStatus,
    RecordRuleV4DeploymentStrategy,
    RecordRuleV4DesiredStatus,
    RecordRuleV4FlowActionType,
    RecordRuleV4FlowStatus,
    RecordRuleV4InputType,
    RecordRuleV4Status,
)
from metadata.models.record_rule.v4 import (
    CONDITION_FALSE,
    CONDITION_FLOW_HEALTHY,
    CONDITION_RECONCILED,
    CONDITION_RESOLVED,
    EVENT_REASON_OPERATION_LOCKED,
    EVENT_STATUS_SKIPPED,
    EVENT_TYPE_OPERATION_SKIPPED,
    RecordRuleV4,
    RecordRuleV4Deployment,
    RecordRuleV4Event,
)
from metadata.models.record_rule.v4.operator import RecordRuleV4Operator
from metadata.models.record_rule.v4.resolver import RecordRuleV4Resolver

pytestmark = pytest.mark.django_db(databases="__all__")

TENANT_ID = "system"
SPACE_TYPE = "bkcc"
SPACE_ID = "2"
GROUP_NAME = "rr_cpu_group"
SOURCE_TABLE_ID = "system.cpu_summary"
SOURCE_VM_TABLE_ID = "2_system_cpu_summary"
METRICQL = 'avg by (bk_target_ip) ({bk_biz_id="2", result_table_id="system.cpu_summary", __name__="usage"})'
CHANGED_METRICQL = f"sum({METRICQL})"


@pytest.fixture
def v4_base_data(settings):
    settings.DEFAULT_BKDATA_BIZ_ID = 2
    settings.BK_DATA_PROJECT_MAINTAINER = "admin"
    models.Space.objects.create(
        bk_tenant_id=TENANT_ID,
        space_type_id=SPACE_TYPE,
        space_id=SPACE_ID,
        space_name="biz-2",
    )
    cluster = models.ClusterInfo.objects.create(
        bk_tenant_id=TENANT_ID,
        cluster_id=1001,
        cluster_name="monitor-opsystem",
        cluster_type=models.ClusterInfo.TYPE_VM,
        domain_name="vm.service.local",
        port=9090,
        description="default vm",
        is_default_cluster=True,
    )
    models.AccessVMRecord.objects.create(
        bk_tenant_id=TENANT_ID,
        result_table_id=SOURCE_TABLE_ID,
        bk_base_data_id=100,
        vm_result_table_id=SOURCE_VM_TABLE_ID,
        vm_cluster_id=cluster.cluster_id,
    )
    return SimpleNamespace(cluster=cluster)


@pytest.fixture
def external_api(mocker):
    check_query_ts = mocker.patch(
        "metadata.models.record_rule.v4.resolver.api.unify_query.check_query_ts",
        return_value=build_check_result(),
    )
    check_promql = mocker.patch(
        "metadata.models.record_rule.v4.resolver.api.unify_query.check_query_ts_by_promql",
        return_value=build_check_result(metricql="promql_metricql"),
    )
    apply_data_link = mocker.patch(
        "metadata.models.record_rule.v4.deployment.runner.api.bkdata.apply_data_link",
        return_value={"status": "ok"},
    )
    delete_data_link = mocker.patch(
        "metadata.models.record_rule.v4.deployment.runner.api.bkdata.delete_data_link",
        return_value={"status": "deleted"},
    )
    get_data_link = mocker.patch(
        "metadata.models.record_rule.v4.deployment.runner.api.bkdata.get_data_link",
        return_value={"status": {"state": "ok"}},
    )
    return SimpleNamespace(
        check_query_ts=check_query_ts,
        check_promql=check_promql,
        apply_data_link=apply_data_link,
        delete_data_link=delete_data_link,
        get_data_link=get_data_link,
    )


def build_check_result(metricql: str = METRICQL, result_table_id: str = SOURCE_TABLE_ID) -> dict:
    return {
        "data": [
            {
                "storage_type": "victoria_metrics",
                "metricql": metricql,
                "result_table_id": [result_table_id],
            }
        ],
        "route_info": [
            {
                "reference_name": "a",
                "metric_name": "usage",
                "table_id": result_table_id,
                "db": "system",
                "measurement": "cpu_summary",
                "data_source": "bk_monitor",
                "storage_type": "victoria_metrics",
                "storage_id": "victoria_metrics",
            }
        ],
    }


def build_query_config() -> dict:
    return {
        "query_list": [
            {
                "data_source": "bk_monitor",
                "table_id": "system.cpu_summary",
                "field_name": "usage",
                "reference_name": "a",
            }
        ],
        "metric_merge": "a",
        "start_time": "1710000000",
        "end_time": "1710000600",
        "step": "1m",
    }


def build_record(
    *,
    record_name: str = "cpu_usage",
    metric_name: str = "cpu_usage_avg",
    input_type: str = RecordRuleV4InputType.QUERY_TS.value,
    input_config: dict | None = None,
    record_key: str = "",
) -> dict:
    record = {
        "record_name": record_name,
        "input_type": input_type,
        "input_config": input_config or build_query_config(),
        "metric_name": metric_name,
        "labels": [{"scenario": "pytest"}],
        "interval": "1min",
    }
    if record_key:
        record["record_key"] = record_key
    return record


def create_rule(
    *,
    records: list[dict] | None = None,
    strategy: str = RecordRuleV4DeploymentStrategy.PER_RECORD.value,
    auto_refresh: bool = True,
    apply_immediately: bool = True,
) -> RecordRuleV4:
    records = records or [build_record()]
    return RecordRuleV4Operator.create(
        bk_tenant_id=TENANT_ID,
        space_type=SPACE_TYPE,
        space_id=SPACE_ID,
        group_name=GROUP_NAME,
        records=records,
        raw_config={"records": records},
        deployment_strategy=strategy,
        auto_refresh=auto_refresh,
        source="pytest",
        operator="tester",
        apply_immediately=apply_immediately,
    )


def get_recording_rule_node(flow_config: dict) -> dict:
    for node in flow_config["spec"]["nodes"]:
        if node["kind"] == "RecordingRuleNode":
            return node
    raise AssertionError("RecordingRuleNode not found")


def test_create_group_with_two_records_applies_per_record_flows(v4_base_data, external_api):
    records = [
        build_record(record_name="cpu_usage", metric_name="cpu_usage_avg"),
        build_record(record_name="cpu_total", metric_name="cpu_total_sum"),
    ]

    rule = create_rule(records=records)

    rule.refresh_from_db()
    assert len(rule.table_id) <= 50
    assert len(rule.dst_vm_table_id) <= 50
    assert rule.table_id.startswith("bkprecal_rr_cpu_group_")
    assert rule.latest_resolved.records.count() == 2
    assert rule.latest_resolved.flows.count() == 2
    assert len(rule.latest_deployment.plan_config["actions"]) == 2
    assert rule.latest_deployment_id == rule.applied_deployment_id
    assert rule.status == RecordRuleV4Status.RUNNING.value
    assert rule.update_available is False

    flows = list(rule.latest_resolved.flows.order_by("id"))
    assert {flow.table_id for flow in flows} == {rule.table_id}
    assert {flow.dst_vm_table_id for flow in flows} == {rule.dst_vm_table_id}
    assert len({flow.flow_name for flow in flows}) == 2
    assert all(len(flow.flow_name) <= 50 for flow in flows)
    assert all(flow.records.count() == 1 for flow in flows)

    first_node = get_recording_rule_node(flows[0].flow_config)
    assert first_node["inputs"] == ["vm_source"]
    assert first_node["output"] == rule.dst_vm_table_id
    assert first_node["storage"] == {
        "kind": "VmStorage",
        "tenant": RECORD_RULE_V4_DEFAULT_TENANT,
        "namespace": RECORD_RULE_V4_BKMONITOR_NAMESPACE,
        "name": "monitor-opsystem",
    }
    assert flows[0].flow_config["metadata"]["namespace"] == RECORD_RULE_V4_BKBASE_NAMESPACE

    assert external_api.check_query_ts.call_count == 2
    assert external_api.apply_data_link.call_count == 2
    assert models.ResultTable.objects.filter(table_id=rule.table_id, bk_tenant_id=TENANT_ID).exists()
    assert models.ResultTableField.objects.filter(
        table_id=rule.table_id,
        bk_tenant_id=TENANT_ID,
        field_name="cpu_usage_avg",
    ).exists()
    assert models.ResultTableField.objects.filter(
        table_id=rule.table_id,
        bk_tenant_id=TENANT_ID,
        field_name="cpu_total_sum",
    ).exists()
    assert models.AccessVMRecord.objects.filter(
        result_table_id=rule.table_id,
        vm_result_table_id=rule.dst_vm_table_id,
        bk_tenant_id=TENANT_ID,
        vm_cluster_id=v4_base_data.cluster.cluster_id,
    ).exists()


def test_single_flow_strategy_groups_records_into_one_flow(v4_base_data, external_api):
    records = [
        build_record(record_name="cpu_usage", metric_name="cpu_usage_avg"),
        build_record(record_name="cpu_total", metric_name="cpu_total_sum"),
    ]

    rule = create_rule(records=records, strategy=RecordRuleV4DeploymentStrategy.SINGLE_FLOW.value)

    flow = rule.latest_resolved.flows.get()
    recording_rule_node = get_recording_rule_node(flow.flow_config)
    assert rule.latest_resolved.records.count() == 2
    assert flow.flow_key == "group"
    assert flow.records.count() == 2
    assert [item["metric_name"] for item in recording_rule_node["config"]] == ["cpu_usage_avg", "cpu_total_sum"]
    assert external_api.apply_data_link.call_count == 1


def test_create_allows_duplicate_group_name_with_random_output_names(v4_base_data, external_api):
    first = create_rule(apply_immediately=False)
    second = create_rule(apply_immediately=False)

    assert first.group_name == second.group_name == GROUP_NAME
    assert first.table_id != second.table_id
    assert first.dst_vm_table_id != second.dst_vm_table_id


def test_create_prepares_output_metadata_before_apply(v4_base_data, external_api):
    rule = create_rule(apply_immediately=False)

    assert models.ResultTable.objects.filter(table_id=rule.table_id, bk_tenant_id=TENANT_ID).exists()
    assert models.AccessVMRecord.objects.filter(
        result_table_id=rule.table_id,
        vm_result_table_id=rule.dst_vm_table_id,
        bk_tenant_id=TENANT_ID,
        vm_cluster_id=v4_base_data.cluster.cluster_id,
    ).exists()
    assert models.ResultTableField.objects.filter(
        table_id=rule.table_id,
        bk_tenant_id=TENANT_ID,
        field_name="cpu_usage_avg",
    ).exists()
    external_api.apply_data_link.assert_not_called()


def test_spec_record_key_is_inherited_by_identity_when_input_key_is_hidden(v4_base_data, external_api):
    rule = create_rule(apply_immediately=False)
    original_record = rule.current_spec.records.get()

    changed_record = build_record(input_config={**build_query_config(), "step": "5m"})
    RecordRuleV4Operator(rule, source="manual", operator="admin").update_spec(
        records=[changed_record],
        raw_config={"records": [changed_record]},
        apply_immediately=False,
    )

    rule.refresh_from_db()
    next_record = rule.current_spec.records.get()
    assert next_record.record_key == original_record.record_key
    assert next_record.identity_hash == original_record.identity_hash
    assert next_record.content_hash != original_record.content_hash


def test_run_check_dispatches_promql_input_to_promql_api(v4_base_data, external_api):
    record = build_record(
        record_name="cpu_promql",
        input_type=RecordRuleV4InputType.PROMQL.value,
        input_config={"promql": "sum(cpu_usage)", "start": "1", "end": "2"},
        metric_name="cpu_usage_sum",
    )
    rule = create_rule(records=[record], apply_immediately=False)
    spec_record = rule.current_spec.records.get()
    external_api.check_query_ts.reset_mock()
    external_api.check_promql.reset_mock()

    result = RecordRuleV4Resolver(rule, source="manual").run_check(spec_record)

    assert result["data"][0]["metricql"] == "promql_metricql"
    external_api.check_promql.assert_called_once_with(
        bk_tenant_id=TENANT_ID,
        promql="sum(cpu_usage)",
        start="1",
        end="2",
    )
    external_api.check_query_ts.assert_not_called()


def test_manual_refresh_only_marks_update_available(v4_base_data, external_api):
    rule = create_rule()
    applied_deployment_id = rule.applied_deployment_id
    external_api.apply_data_link.reset_mock()
    external_api.check_query_ts.return_value = build_check_result(metricql=CHANGED_METRICQL)

    resolved = RecordRuleV4Operator(rule, source="manual", operator="admin").manual_refresh()

    rule.refresh_from_db()
    assert resolved is not None
    assert rule.latest_resolved_id == resolved.pk
    assert rule.applied_deployment_id == applied_deployment_id
    assert rule.latest_deployment_id != applied_deployment_id
    assert rule.update_available is True
    assert rule.status == RecordRuleV4Status.PENDING.value
    external_api.apply_data_link.assert_not_called()


def test_resolved_unchanged_does_not_replan_because_flow_template_is_not_the_comparison_source(
    v4_base_data, external_api
):
    rule = create_rule(auto_refresh=True)
    latest_resolved_id = rule.latest_resolved_id
    latest_deployment_id = rule.latest_deployment_id
    external_api.apply_data_link.reset_mock()

    resolved = RecordRuleV4Operator(rule, source="scheduler").manual_refresh()

    rule.refresh_from_db()
    assert resolved.pk == latest_resolved_id
    assert rule.latest_deployment_id == latest_deployment_id
    assert rule.update_available is False
    assert external_api.apply_data_link.call_count == 0


def test_manual_refresh_skips_when_operation_lock_is_held(v4_base_data, external_api):
    rule = create_rule()
    token = rule.acquire_operation_lock(owner="scheduler", reason="reconcile", ttl_seconds=60)
    assert token
    external_api.check_query_ts.reset_mock()

    resolved = RecordRuleV4Operator(rule, source="manual", operator="admin").manual_refresh()

    assert resolved is None
    external_api.check_query_ts.assert_not_called()
    event = RecordRuleV4Event.objects.get(
        rule=rule,
        event_type=EVENT_TYPE_OPERATION_SKIPPED,
        status=EVENT_STATUS_SKIPPED,
        reason=EVENT_REASON_OPERATION_LOCKED,
    )
    assert event.detail["operation"] == "manual_refresh"
    assert event.detail["owner"] == "scheduler"
    assert event.detail["reason"] == "reconcile"
    rule.release_operation_lock(token)


def test_reconcile_does_not_apply_when_auto_refresh_is_disabled(v4_base_data, external_api):
    rule = create_rule(auto_refresh=False)
    external_api.apply_data_link.reset_mock()
    external_api.check_query_ts.return_value = build_check_result(metricql=CHANGED_METRICQL)

    changed = RecordRuleV4Operator(rule, source="scheduler").reconcile()

    rule.refresh_from_db()
    assert changed is True
    assert rule.update_available is True
    assert rule.applied_deployment_id != rule.latest_deployment_id
    assert rule.status == RecordRuleV4Status.OUTDATED.value
    external_api.apply_data_link.assert_not_called()


def test_reconcile_applies_changed_resolved_when_auto_refresh_is_enabled(v4_base_data, external_api):
    rule = create_rule(auto_refresh=True)
    external_api.apply_data_link.reset_mock()
    external_api.check_query_ts.return_value = build_check_result(metricql=CHANGED_METRICQL)

    changed = RecordRuleV4Operator(rule, source="scheduler").reconcile()

    rule.refresh_from_db()
    assert changed is True
    assert rule.update_available is False
    assert rule.applied_deployment_id == rule.latest_deployment_id
    assert rule.latest_resolved.records.get().metricql == [CHANGED_METRICQL]
    external_api.apply_data_link.assert_called_once()


def test_stop_updates_runtime_status_without_new_spec_resolved_or_plan(v4_base_data, external_api):
    rule = create_rule()
    previous_spec_id = rule.current_spec_id
    previous_resolved_id = rule.latest_resolved_id
    previous_deployment_id = rule.latest_deployment_id
    external_api.check_query_ts.reset_mock()
    external_api.apply_data_link.reset_mock()

    RecordRuleV4Operator(rule, source="manual", operator="admin").update_spec(
        desired_status=RecordRuleV4DesiredStatus.STOPPED.value
    )

    rule.refresh_from_db()
    flow = rule.latest_resolved.flows.get()
    assert rule.generation == 1
    assert rule.observed_generation == 1
    assert rule.current_spec_id == previous_spec_id
    assert rule.latest_resolved_id == previous_resolved_id
    assert rule.latest_deployment_id == previous_deployment_id
    assert rule.desired_status == RecordRuleV4DesiredStatus.STOPPED.value
    assert flow.desired_status == RecordRuleV4DesiredStatus.STOPPED.value
    assert flow.flow_config["spec"]["desired_status"] == RecordRuleV4DesiredStatus.STOPPED.value
    assert rule.status == RecordRuleV4Status.STOPPED.value
    external_api.check_query_ts.assert_not_called()
    external_api.apply_data_link.assert_called_once()


def test_delete_creates_delete_actions_for_applied_flows(v4_base_data, external_api):
    records = [
        build_record(record_name="cpu_usage", metric_name="cpu_usage_avg"),
        build_record(record_name="cpu_total", metric_name="cpu_total_sum"),
    ]
    rule = create_rule(records=records)
    applied_flow_names = sorted(rule.applied_deployment.resolved.flows.values_list("flow_name", flat=True))

    RecordRuleV4Operator(rule, source="manual", operator="admin").delete()

    rule.refresh_from_db()
    delete_actions = sorted(rule.latest_deployment.plan_config["actions"], key=lambda action: action["flow_name"])
    assert rule.desired_status == RecordRuleV4DesiredStatus.DELETED.value
    assert rule.status == RecordRuleV4Status.DELETED.value
    assert rule.deleted_at is not None
    assert [action["flow_name"] for action in delete_actions] == applied_flow_names
    assert all(action["action_type"] == RecordRuleV4FlowActionType.DELETE.value for action in delete_actions)
    assert external_api.delete_data_link.call_count == 2


def test_apply_failure_keeps_update_available_and_records_action_error(v4_base_data, external_api):
    rule = create_rule(apply_immediately=False)
    external_api.apply_data_link.side_effect = RuntimeError("bkbase unavailable")

    ok = RecordRuleV4Operator(rule, source="manual", operator="admin").apply()

    rule.refresh_from_db()
    deployment = RecordRuleV4Deployment.objects.get(pk=rule.latest_deployment_id)
    assert ok is False
    assert deployment.apply_status == RecordRuleV4ApplyStatus.FAILED.value
    assert rule.update_available is True
    assert rule.last_error == "bkbase unavailable"
    assert rule.get_condition(CONDITION_RECONCILED)["status"] == CONDITION_FALSE
    assert RecordRuleV4Event.objects.filter(deployment=deployment, event_type="flow_action.failed").exists()


def test_apply_skips_stale_deployment_before_calling_bkbase(v4_base_data, external_api):
    rule = create_rule(apply_immediately=False)
    stale_deployment = rule.latest_deployment
    RecordRuleV4Operator(rule, source="manual", operator="admin").update_spec(
        records=[build_record(metric_name="cpu_usage_v2")],
        raw_config={"records": [build_record(metric_name="cpu_usage_v2")]},
        apply_immediately=False,
    )
    assert models.ResultTableField.objects.filter(
        table_id=rule.table_id,
        bk_tenant_id=TENANT_ID,
        field_name="cpu_usage_v2",
    ).exists()
    external_api.apply_data_link.reset_mock()

    ok = RecordRuleV4Operator(rule, source="manual", operator="admin").apply(stale_deployment)

    assert ok is False
    external_api.apply_data_link.assert_not_called()


def test_resolve_failure_keeps_last_applied_deployment(v4_base_data, external_api):
    rule = create_rule()
    applied_deployment_id = rule.applied_deployment_id
    latest_resolved_id = rule.latest_resolved_id
    external_api.check_query_ts.side_effect = RuntimeError("unify-query unavailable")

    resolved = RecordRuleV4Operator(rule, source="scheduler").manual_refresh()

    rule.refresh_from_db()
    assert resolved is None
    assert rule.applied_deployment_id == applied_deployment_id
    assert rule.latest_resolved_id == latest_resolved_id
    assert rule.last_error == "unify-query unavailable"
    assert rule.get_condition(CONDITION_RESOLVED)["status"] == CONDITION_FALSE
    assert rule.status == RecordRuleV4Status.FAILED.value


def test_self_referenced_precalculated_vm_table_is_excluded_from_source(v4_base_data, external_api):
    rule = create_rule()
    resolver = RecordRuleV4Resolver(rule, source="manual")

    src_vm_table_ids = resolver.normalize_src_vm_table_ids(
        [SOURCE_TABLE_ID, SOURCE_VM_TABLE_ID, rule.table_id, rule.dst_vm_table_id]
    )

    assert src_vm_table_ids == [SOURCE_VM_TABLE_ID]


def test_refresh_flow_health_maps_each_flow_status_to_group_condition(v4_base_data, external_api):
    rule = create_rule()
    external_api.get_data_link.return_value = {"status": {"state": "not-ok"}}

    status = RecordRuleV4Operator(rule, source="scheduler").refresh_flow_health()

    rule.refresh_from_db()
    assert status == RecordRuleV4FlowStatus.ABNORMAL.value
    assert rule.get_condition(CONDITION_FLOW_HEALTHY)["status"] == CONDITION_FALSE
    assert rule.status == RecordRuleV4Status.FAILED.value
    assert rule.applied_deployment.resolved.flows.get().flow_status == RecordRuleV4FlowStatus.ABNORMAL.value

    external_api.get_data_link.side_effect = RuntimeError("404 not found")
    status = RecordRuleV4Operator(rule, source="scheduler").refresh_flow_health()

    rule.refresh_from_db()
    assert status == RecordRuleV4FlowStatus.NOT_FOUND.value
    assert rule.get_condition(CONDITION_FLOW_HEALTHY)["reason"] == RecordRuleV4FlowStatus.NOT_FOUND.value


def test_duplicate_record_identity_is_rejected(v4_base_data, external_api):
    duplicated = [
        build_record(record_name="cpu_usage", metric_name="cpu_usage_avg"),
        build_record(record_name="cpu_usage", metric_name="cpu_usage_avg"),
    ]

    with pytest.raises(ValueError):
        create_rule(records=duplicated, apply_immediately=False)
