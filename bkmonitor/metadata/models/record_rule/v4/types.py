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

from typing import Any, TypedDict


class RecordRuleV4RecordInput(TypedDict, total=False):
    """Operator.create/update_spec 接收的单条预计算 record。

    必填字段由模型层继续校验：record_name、input_type、input_config、metric_name。
    record_key 仅用于显式维持记录身份；SCode 等模式不传时会按 identity_hash 继承。
    """

    record_key: str
    record_name: str
    input_type: str
    input_config: dict[str, Any]
    metric_name: str
    labels: list[dict[str, str]]


class StructuredQueryMetricInput(TypedDict, total=False):
    """SaaS 结构化 query_config.metrics 的最小输入形态。"""

    field: str
    method: str
    alias: str


class StructuredQueryConditionInput(TypedDict, total=False):
    """SaaS 结构化 query_config.where 的最小输入形态。"""

    condition: str
    key: str
    method: str
    value: list[Any]


class StructuredQueryFunctionParamInput(TypedDict, total=False):
    """SaaS 查询函数参数。"""

    id: str
    value: Any


class StructuredQueryFunctionInput(TypedDict, total=False):
    """SaaS 查询函数输入。"""

    id: str
    params: list[StructuredQueryFunctionParamInput]


class StructuredQueryConfigInput(TypedDict, total=False):
    """SaaS 结构化 query_configs 的单个查询项。"""

    data_source_label: str
    data_type_label: str
    metrics: list[StructuredQueryMetricInput]
    table: str
    data_label: str
    index_set_id: int | None
    group_by: list[str]
    where: list[StructuredQueryConditionInput]
    interval: int
    interval_unit: str
    time_field: str | None
    filter_dict: dict[str, Any]
    functions: list[StructuredQueryFunctionInput]


class StructuredQueryInput(TypedDict, total=False):
    """用户结构化查询输入，resolver 会将其转换成 /check/query/ts 参数。"""

    bk_biz_id: int
    query_configs: list[StructuredQueryConfigInput]
    expression: str
    functions: list[StructuredQueryFunctionInput]
    start_time: int | str
    end_time: int | str
    limit: int
    slimit: int
    down_sample_range: str
    timezone: str
    instant: bool
    reference: bool
    not_time_align: bool
    add_dimensions: list[str]
