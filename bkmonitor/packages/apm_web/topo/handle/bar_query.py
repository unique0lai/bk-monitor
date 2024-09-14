# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2022 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""
import copy
import itertools
import json
import urllib.parse
from dataclasses import asdict, dataclass, field
from typing import Dict, List
from urllib.parse import urljoin

from django.conf import settings

from apm_web.constants import AlertLevel, DataStatus
from apm_web.handlers.compatible import CompatibleQuery
from apm_web.metric_handler import (
    ApdexRange,
    ServiceFlowErrorRate,
    ServiceFlowErrorRateCallee,
    ServiceFlowErrorRateCaller,
)
from apm_web.models import Application
from apm_web.topo.constants import BarChartDataType
from apm_web.topo.handle import BaseQuery
from apm_web.utils import get_bar_interval_number
from core.drf_resource import resource
from monitor_web.models.scene_view import SceneViewModel
from monitor_web.scene_view.builtin.apm import ApmBuiltinProcessor


@dataclass
class BarSeries:
    datapoints: List = field(default_factory=list)
    dimensions: Dict = field(default_factory=dict)
    target: str = ""
    type: str = "bar"
    unit: str = ""


@dataclass
class BarResponse:
    metrics: List = field(default_factory=list)
    series: List[BarSeries] = field(default_factory=list)


class BarQuery(BaseQuery):
    def execute(self) -> dict:
        if not self.params.get("endpoint_name"):
            if self.application.data_status == DataStatus.NO_DATA and self.data_type != BarChartDataType.Alert.value:
                # 如果应用无数据 则柱状图显示为无数据
                return asdict(BarResponse())

            return getattr(self, f"get_{self.data_type}_series")()
        else:
            if not self.service_name:
                raise ValueError(f"[柱状图] 查询接口: {self.params['endpoint_name']} 的告警数据时需要传递服务名称")
            # 接口目前只能查询 告警 / Apdex
            if self.data_type == BarChartDataType.Alert.value:
                return self.get_alert_series()
            if self.data_type == BarChartDataType.Apdex.value:
                return self.get_apdex_series()

            raise ValueError(f"[柱状图] 不支持查询接口: {self.params['endpoint_name']} 的 {self.data_type} 数据")

    def get_alert_series(self) -> Dict:
        ts_mapping = {AlertLevel.INFO: {}, AlertLevel.WARN: {}, AlertLevel.ERROR: {}}
        all_ts = []
        query_string = CompatibleQuery.get_alert_query_string(
            self.metrics_table,
            self.bk_biz_id,
            self.app_name,
            self.service_name,
            self.params.get("endpoint_name"),
        )
        common_params = {
            "bk_biz_ids": [self.bk_biz_id],
            "start_time": self.start_time,
            "end_time": self.end_time,
            "interval": get_bar_interval_number(self.start_time, self.end_time),
            "query_string": query_string,
            "conditions": [],
        }

        if self.params.get("strategy_ids", []):
            common_params["conditions"].append({"key": "strategy_id", "value": self.params["strategy_ids"]})

        for level in [AlertLevel.INFO, AlertLevel.WARN, AlertLevel.ERROR]:
            params = copy.deepcopy(common_params)
            params["conditions"].append({"key": "severity", "value": [level]})
            alert_series = resource.fta_web.alert.alert_date_histogram(params)
            if not all_ts and alert_series.get("series", []):
                all_ts = sorted(
                    set(itertools.chain(*[[j[0] for j in i.get("data", [])] for i in alert_series["series"]]))
                )

            ts_mapping[level] = {
                j[0]: j[1]
                for i in alert_series.get("series", [])
                if i.get("name") == "ABNORMAL"
                for j in i.get("data", [])
            }

        res = []
        for t in all_ts:
            info_count = ts_mapping[AlertLevel.INFO].get(t, 0)
            warn_count = ts_mapping[AlertLevel.WARN].get(t, 0)
            error_count = ts_mapping[AlertLevel.ERROR].get(t, 0)
            if error_count > 0:
                # 致命级别优先级最高
                res.append([[1, error_count], t])
            elif info_count > 0 or warn_count > 0:
                res.append([[2, info_count + warn_count], t])
            else:
                res.append([[3, 0], t])

        return asdict(BarResponse(series=[BarSeries(datapoints=[res])]))

    def get_apdex_series(self) -> Dict:
        return self.get_metric(
            ApdexRange,
            interval=get_bar_interval_number(self.start_time, self.end_time),
            where=CompatibleQuery.list_metric_wheres(
                self.bk_biz_id,
                self.app_name,
                self.service_name,
                self.params.get("endpoint_name"),
            ),
        ).query_range()

    def get_error_rate_series(self) -> Dict:
        return self.get_metric(
            ServiceFlowErrorRate,
            interval=get_bar_interval_number(self.start_time, self.end_time),
            where=CompatibleQuery.list_flow_metric_wheres(mode="full", service_name=self.service_name),
        ).query_range()

    def get_error_rate_caller_series(self) -> Dict:
        return self.get_metric(
            ServiceFlowErrorRateCaller,
            interval=get_bar_interval_number(self.start_time, self.end_time),
            where=CompatibleQuery.list_flow_metric_wheres(mode="caller", service_name=self.service_name),
        ).query_range()

    def get_error_rate_callee_series(self) -> Dict:
        return self.get_metric(
            ServiceFlowErrorRateCallee,
            interval=get_bar_interval_number(self.start_time, self.end_time),
            where=CompatibleQuery.list_flow_metric_wheres(mode="callee", service_name=self.service_name),
        ).query_range()


class LinkHelper:
    @classmethod
    def get_service_alert_link(cls, bk_biz_id, app_name, service_name, start_time, end_time):
        """获取服务的告警中心链接"""
        table_id = Application.objects.filter(bk_biz_id=bk_biz_id, app_name=app_name).get().metric_result_table_id
        return (
            f"/?bizId={bk_biz_id}#/event-center?"
            f"queryString={CompatibleQuery.get_alert_query_string(table_id, bk_biz_id, app_name, service_name)}&"
            f"from={start_time * 1000}&to={end_time * 1000}"
        )

    @classmethod
    def get_endpoint_alert_link(cls, bk_biz_id, app_name, service_name, endpoint_name, start_time, end_time):
        """获取接口的告警中心链接"""
        table_id = Application.objects.filter(bk_biz_id=bk_biz_id, app_name=app_name).get().metric_result_table_id
        query_string = CompatibleQuery.get_alert_query_string(
            table_id, bk_biz_id, app_name, service_name, endpoint_name
        )
        return (
            f"/?bizId={bk_biz_id}#/event-center?"
            f"queryString={query_string}&"
            f"from={start_time * 1000}&to={end_time * 1000}"
        )

    @classmethod
    def get_service_log_tab_link(cls, bk_biz_id, app_name, service_name, start_time, end_time, views=None):
        """获取服务的日志 tab 页面链接"""
        if not views:
            views = SceneViewModel.objects.filter(bk_biz_id=bk_biz_id, scene_id="apm_service")

        dashboard_id = ApmBuiltinProcessor.get_dashboard_id(bk_biz_id, app_name, service_name, "log", views)
        if not dashboard_id:
            return None

        return (
            f"/service?"
            f"filter-service_name={service_name}&"
            f"filter-app_name={app_name}&"
            f"from={start_time * 1000}&"
            f"to={end_time * 1000}&"
            f"dashboardId={dashboard_id}"
        )

    @classmethod
    def get_service_overview_tab_link(cls, bk_biz_id, app_name, service_name, start_time, end_time, views=None):
        """获取服务的概览 tab 页面链接"""
        if not views:
            views = SceneViewModel.objects.filter(bk_biz_id=bk_biz_id, scene_id="apm_service")

        dashboard_id = ApmBuiltinProcessor.get_dashboard_id(bk_biz_id, app_name, service_name, "overview", views)
        if not dashboard_id:
            return None

        return (
            f"/service?"
            f"filter-service_name={service_name}&"
            f"filter-app_name={app_name}&"
            f"from={start_time * 1000}&"
            f"to={end_time * 1000}&"
            f"dashboardId={dashboard_id}"
        )

    @classmethod
    def get_host_monitor_link(cls, bk_host_id, start_time, end_time):
        """获取某主机的主机监控地址"""
        return f"/performance/detail/{bk_host_id}?from={start_time * 1000}&to={end_time * 1000}"

    @classmethod
    def get_pod_monitor_link(cls, bcs_cluster_id, namespace, pod, start_time, end_time):
        """获取某 Pod 的 K8S 监控地址"""
        query_data = {
            "selectorSearch": [
                {
                    "keyword": pod,
                }
            ]
        }
        encode_query = urllib.parse.quote(json.dumps(query_data))

        return (
            f"/k8s?filter-bcs_cluster_id={bcs_cluster_id}&"
            f"filter-namespace={namespace}&"
            f"filter-pod_name={pod}&dashboardId=pod&sceneId=kubernetes&sceneType=detail&"
            f"from={start_time * 1000}&to={end_time * 1000}&"
            f"queryData={encode_query}"
        )

    @classmethod
    def get_service_monitor_link(cls, bcs_cluster_id, namespace, service, start_time, end_time):
        """获取某 Service 的 K8S 监控地址"""
        query_data = {
            "selectorSearch": [
                {
                    "keyword": service,
                }
            ]
        }
        encode_query = urllib.parse.quote(json.dumps(query_data))

        return (
            f"/k8s?filter-bcs_cluster_id={bcs_cluster_id}&"
            f"filter-namespace={namespace}&"
            f"filter-service_name={service}&"
            f"from={start_time * 1000}&to={end_time * 1000}&"
            f"dashboardId=service&sceneId=kubernetes&sceneType=detail&queryData={encode_query}"
        )

    @classmethod
    def get_host_cmdb_link(cls, bk_biz_id, bk_host_id):
        """获取主机在 cmdb 中的链接"""
        return urljoin(settings.BK_CC_URL, f"#/business/{bk_biz_id}/index/host/{bk_host_id}")
