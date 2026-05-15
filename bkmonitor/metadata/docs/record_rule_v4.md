# V4 Recording Rule 预计算方案

## 背景

Metadata 当前已有 `RecordRule` 预计算链路，主要面向 bkbase V3 计算 flow：

- `RecordRule` 保存规则、源 VM RT、目标 VM RT、计算频率和状态。
- `ResultTableFlow` 负责拼装 V3 flow 的 source / promql_v2 / vm_storage 节点，并调用 bkdata V3 flow API。
- 规则解析依赖 `unify_query.promql_to_struct` / `struct_to_promql`，最终生成 V3 flow 可消费的 SQL 配置。

新的预计算模块面向 bkbase V4 链路，入口不再直接复用旧的 SQL 转换逻辑，而是先通过 unify-query 的 check 接口做只解析、只预览的路由与 MetricQL 生成，再基于返回信息创建 V4 recording rule flow。

## Unify Query 解析预览

V4 预计算支持两种用户输入方式，分别对应 unify-query 的两个 check 接口：

| 输入方式 | SaaS API Resource | unify-query path | 说明 |
| --- | --- | --- | --- |
| 结构化 QueryTs | `api.unify_query.check_query_ts` | `POST /check/query/ts` | 直接提交 `query_list`、`metric_merge`、时间范围等结构化参数 |
| PromQL | `api.unify_query.check_query_ts_by_promql` | `POST /check/query/ts/promql` | 提交 PromQL，由 unify-query 内部转换成 QueryTs 后走同一套 check 逻辑 |

公共 header 由 SaaS API wrapper 处理，check resource 暂不额外暴露公共 header 透传参数：

- `Content-Type: application/json`
- `X-Bk-Scope-Space-Uid`: 优先由 `space_uid` 入参推导，或沿用现有 wrapper 从请求上下文、`bk_biz_ids` 推导的逻辑
- `X-Bk-Scope-Skip-Space`: 沿用现有全局空间逻辑

### 结构化 QueryTs

必填字段：

- `query_list`
- `start_time`
- `end_time`

当前 SaaS resource 暴露的可选字段：

- `metric_merge`
- `step`
- `space_uid`
- `timezone`
- `instant`
- `reference`
- `not_time_align`
- `limit`

最小请求示例：

```json
{
  "space_uid": "bkcc__<BK_BIZ_ID>",
  "query_list": [
    {
      "data_source": "bk_monitor",
      "table_id": "<RESULT_TABLE_ID>",
      "field_name": "<METRIC_NAME>",
      "is_regexp": false,
      "function": [
        {
          "method": "avg",
          "dimensions": ["bk_target_ip"]
        }
      ],
      "time_aggregation": {
        "function": "avg_over_time",
        "window": "1m"
      },
      "conditions": {
        "field_list": [
          {
            "field_name": "bk_biz_id",
            "value": ["<BK_BIZ_ID>"],
            "op": "eq"
          }
        ],
        "condition_list": []
      },
      "table_id_conditions": [],
      "reference_name": "a"
    }
  ],
  "metric_merge": "a",
  "start_time": "1710000000",
  "end_time": "1710000600",
  "step": "1m",
  "timezone": "Asia/Shanghai",
  "instant": false,
  "reference": false,
  "not_time_align": false,
  "limit": 0
}
```

### PromQL

必填字段：

- `promql`
- `start`
- `end`

当前 SaaS resource 暴露的可选字段：

- `step`
- `bk_biz_ids`
- `limit`
- `slimit`
- `match`
- `is_verify_dimensions`
- `reference`
- `not_time_align`
- `down_sample_range`
- `timezone`
- `look_back_delta`
- `instant`
- `add_dimensions`

最小请求示例：

```json
{
  "promql": "avg by (bk_target_ip) (avg_over_time(<METRIC_NAME>{bk_biz_id=\"<BK_BIZ_ID>\"}[1m]))",
  "start": "1710000000",
  "end": "1710000600",
  "step": "1m",
  "instant": false,
  "timezone": "Asia/Shanghai",
  "look_back_delta": "",
  "reference": false,
  "not_time_align": false,
  "down_sample_range": "",
  "bk_biz_ids": ["<BK_BIZ_ID>"],
  "limit": 0,
  "slimit": 0,
  "match": "",
  "is_verify_dimensions": false,
  "add_dimensions": []
}
```

### 成功响应

两个接口响应结构一致：

```json
{
  "data": [
    {
      "storage_type": "victoria_metrics",
      "metricql": "avg by (bk_target_ip) (avg_over_time({bk_biz_id=\"<BK_BIZ_ID>\", result_table_id=\"<VM_RESULT_TABLE_ID>\", __name__=\"<METRIC_NAME>\"}[1m]))",
      "result_table_id": ["<VM_RESULT_TABLE_ID>"]
    }
  ],
  "route_info": [
    {
      "reference_name": "a",
      "metric_name": "<METRIC_NAME>",
      "table_id": "<RESULT_TABLE_ID>",
      "db": "<DB_NAME>",
      "measurement": "<MEASUREMENT_NAME>",
      "data_label": "",
      "data_source": "bk_monitor",
      "storage_type": "victoria_metrics",
      "storage_id": "victoria_metrics"
    }
  ],
  "trace_id": "xxxx"
}
```

关键约定：

- `data` 中直查 VM 场景通常返回 `storage_type`、`metricql`、`result_table_id`。
- 非直查场景如果底层存储未实现预览体，`data` 可能为空数组。
- 只要 `route_info` 非空，即使 `data: []` 也表示解析和路由预览成功。
- 失败时返回 `trace_id` 与 `error`，调用方应记录两者，方便回溯 unify-query 日志。

## V4 Flow 配置

V4 recording rule flow 使用 `POST /v4/apply/` 创建，删除时使用：

```text
DELETE /v4/namespaces/bkbase/flows/{flow_name}/
```

典型 flow 配置：

```json
{
  "config": [
    {
      "kind": "Flow",
      "metadata": {
        "tenant": "default",
        "namespace": "bkbase",
        "name": "<FLOW_NAME>",
        "labels": {},
        "annotations": {}
      },
      "spec": {
        "nodes": [
          {
            "kind": "VmSourceNode",
            "name": "vm_source",
            "data": {
              "kind": "ResultTable",
              "tenant": "default",
              "namespace": "bkmonitor",
              "name": "<VM_RESULT_TABLE_ID>"
            }
          },
          {
            "kind": "RecordingRuleNode",
            "name": "<FLOW_NAME>",
            "inputs": ["vm_source"],
            "output": "<OUTPUT_RESULT_TABLE>",
            "config": [
              {
                "expr": "sum(rate(<METRIC_NAME>{bk_biz_id=\"<BK_BIZ_ID>\"}[1m]))",
                "interval": "1min",
                "metric_name": "<OUTPUT_METRIC_NAME>",
                "labels": [
                  {
                    "label_key": "label_value"
                  }
                ]
              }
            ],
            "storage": {
              "kind": "VmStorage",
              "tenant": "default",
              "namespace": "bkmonitor",
              "name": "<VM_STORAGE_NAME>"
            }
          }
        ],
        "operation_config": {
          "start_position": "from_head",
          "stream_cluster": null,
          "batch_cluster": null,
          "deploy_mode": null
        },
        "maintainers": ["<MAINTAINER>"],
        "desired_status": "running"
      },
      "status": null
    }
  ]
}
```

已确认约束：

- `VmSourceNode` 的源 RT 必须已有 VM storage。
- 如果 check 返回的源 VM RT 中包含当前预计算自身的输出 RT，需要在生成 `VmSourceNode` 前排除，避免自引用计算。
- `RecordingRuleNode.inputs` 指向一个或多个 `VmSourceNode.name`。
- `RecordingRuleNode.output` 是 recording rule 产生的新 RT，需要提前在 metadata 中定义。
- `interval` 当前支持 `1min`、`2min`、`5min`、`10min`。
- series 限制口径是单个 PromQL 计算的 series 总量，当前约束按 50W 理解。
- recording rule 场景暂不需要关心 `operation_config` 细节，先按固定默认值下发。

## 新预计算表

建议新增独立的 V4 预计算表，避免把 V3 `RecordRule` 的 BkSQL、V3 flow id、旧节点配置语义继续塞进同一张表。

建议字段：

| 字段 | 说明 |
| --- | --- |
| `space_type` / `space_id` / `bk_tenant_id` | 空间与租户 |
| `record_name` | 用户侧预计算名称 |
| `table_id` | metadata 侧逻辑 RT |
| `dst_vm_table_id` | V4 recording rule 输出 RT |
| `input_type` | `promql` 或 `query_ts` |
| `input_config` | 用户原始输入 |
| `check_result` | unify-query check 响应快照，至少保留 `data`、`route_info`、`trace_id` |
| `metricql` | 从 `data[].metricql` 提取的最终计算表达式 |
| `src_vm_table_ids` | 从 `route_info` / `data[].result_table_id` 汇总出的源 VM RT |
| `metric_name` | 输出指标名 |
| `labels` | recording rule 附加标签 |
| `interval` | 计算周期，限定为 `1min`、`2min`、`5min`、`10min` |
| `vm_storage_name` | 输出 VM storage 名称 |
| `flow_name` | V4 flow 名称 |
| `flow_config` | 下发到 `/v4/apply/` 的完整配置 |
| `status` | `created`、`running`、`failed`、`deleted` 等 |
| `last_error` | 最近一次创建、更新或删除失败信息 |
| `last_check_time` | 最近一次调用 unify-query check 的时间 |
| `last_refresh_time` | 最近一次刷新 V4 flow 的时间 |
| `auto_refresh` | 是否允许周期任务在检测到变更后自动刷新 flow |
| `has_change` | 是否存在待更新变更；`auto_refresh=false` 时周期任务只更新该标记，不自动刷新 flow |

索引建议：

- `(bk_tenant_id, space_type, space_id, record_name)` 唯一。
- `(bk_tenant_id, table_id)` 唯一。
- `(bk_tenant_id, flow_name)` 唯一。

## 模块设计

### 核心模型

核心逻辑围绕 `metadata/models/record_rule/v4.py` 中的 `RecordRuleV4` 收敛，避免把 V4 预计算的状态和编排逻辑拆散。

`RecordRuleV4` 建议负责：

- 保存用户原始输入、check 响应快照、当前生效的 `metricql`、`src_vm_table_ids`、输出 RT、flow 名称、flow config、状态和错误信息。
- 根据 `input_type` / `input_config` 调用 `api.unify_query.check_query_ts` 或 `api.unify_query.check_query_ts_by_promql`。
- 从 check 响应中提取并标准化 `metricql`、`src_vm_table_ids`、`route_info`。
- 创建或更新输出 RT、字段和 VM 存储记录。
- 生成 V4 Flow 配置。
- 调用 `api.bkdata.apply_data_link` 创建或更新 V4 flow。
- 调用 `api.bkdata.delete_data_link` 删除 V4 flow。
- 在 `refresh_if_changed` 中完成漂移检查和有变更刷新。

推荐方法边界：

```python
class RecordRuleV4(BaseModelWithTime):
    @classmethod
    def create_rule(cls, ...): ...

    def run_check(self) -> dict: ...
    def build_runtime_config(self, check_result: dict) -> dict: ...
    def compose_flow_config(self, runtime_config: dict) -> dict: ...
    def apply_flow(self) -> None: ...
    def delete_flow(self) -> None: ...
    def refresh_if_changed(self, auto_apply: bool | None = None) -> bool: ...
```

### Resource 门面

在 `metadata/resources/record_rule.py` 新增对外 Resource，作为增删改查和手动刷新入口。Resource 只做参数校验、权限上下文整理和调用模型方法，不直接拼 flow 或调用 bkbase。

建议首期 Resource：

| Resource | 作用 |
| --- | --- |
| `CreateRecordRuleV4Resource` | 创建 V4 预计算规则，内部调用 `RecordRuleV4.create_rule` |
| `ModifyRecordRuleV4Resource` | 修改规则输入、interval、metric_name、labels 等配置，触发重新 check 和 flow 刷新 |
| `DeleteRecordRuleV4Resource` | 删除或停用规则，内部调用 `delete_flow` 并更新状态 |
| `GetRecordRuleV4Resource` | 查询单条规则详情，包含当前 check/flow 状态 |
| `ListRecordRuleV4Resource` | 按空间、状态、名称列表查询 |
| `RefreshRecordRuleV4Resource` | 手动触发 `refresh_if_changed` |

同时在 `metadata/resources/__init__.py` 中导出：

```python
from .record_rule import *  # noqa
```

### API 封装复用

`api/bkdata/default.py` 已有 V4 apply / delete 通用封装，V4 recording rule 不需要新增专用 API Resource：

- `ApplyDataLink` -> `POST /v4/apply/`
- `DeleteDataLink` -> `DELETE /v4/namespaces/{namespace}/{kind}/{name}/`

调用方式保持通用资源语义：

```python
api.bkdata.apply_data_link(config=[flow_config])
api.bkdata.delete_data_link(
    bk_tenant_id=bk_tenant_id,
    namespace="bkbase",
    kind="flows",
    name=flow_name,
)
```

### 定时任务

在 `metadata/task/record_rule_v4.py` 放周期任务薄壳，只负责扫描和触发：

```python
def refresh_record_rule_v4():
    for rule in RecordRuleV4.objects.filter(status=RecordRuleV4Status.RUNNING.value):
        if rule.is_refresh_due():
            rule.refresh_if_changed(auto_apply=rule.auto_refresh)
```

任务层不解析 check、不拼 flow、不直接调用 bkbase，便于保持业务逻辑只在 `RecordRuleV4` 中维护。

## 开发计划

1. API 层
   - 已补 `api.unify_query.check_query_ts` 和 `api.unify_query.check_query_ts_by_promql`。
   - 补充单元测试，覆盖两个 resource 的 path、序列化字段和请求体透传。

2. 元数据模型
   - 新增 `metadata/models/record_rule/v4.py` 和 migration。
   - 明确状态枚举、输入类型枚举、interval 枚举。
   - 复用现有 RT / field / AccessVMRecord 创建能力，但不复用 V3 `bk_sql_config` 字段语义。

3. Metadata Resource
   - 新增 `metadata/resources/record_rule.py`。
   - 暴露创建、修改、删除、详情、列表、手动刷新 Resource。
   - 在 `metadata/resources/__init__.py` 导出新 resource。

4. 解析与校验服务
   - 按 `input_type` 调用对应 unify-query check API。
   - 要求 `route_info` 非空；VM 直查场景要求能得到至少一个 `metricql`。
   - 校验所有源 RT 都有 VM storage。
   - 从源 RT 中排除当前预计算自身的 `table_id` / `dst_vm_table_id`。
   - 校验输出 `metric_name`、`labels`、`interval` 合法。
   - 记录 `trace_id`，失败时把 unify-query 原始错误写入 `last_error`。

5. 输出 RT 预定义
   - 根据空间和预计算名称生成稳定 `table_id`。
   - 创建 ResultTable、ResultTableField、AccessVMRecord。
   - 输出 RT 要在创建 V4 flow 前完成定义。

6. V4 Flow 编排
   - 为每个源 VM RT 生成 `VmSourceNode`。
   - 用 check 返回的 `metricql` 生成 `RecordingRuleNode.config[].expr`。
   - `output` 指向提前定义的输出 RT。
   - `storage` 使用目标空间对应 VM cluster。
   - `operation_config` 先固定为默认配置。
   - 复用 `api.bkdata.apply_data_link` / `api.bkdata.delete_data_link`，不新增 bkdata 专用接口。

7. 生命周期
   - create: 解析预览 -> 建 RT -> 建记录 -> apply flow。
   - start/update: 首期可按重新 apply 或删除重建实现，待确认 V4 API 是否支持原地更新。
   - delete: 删除 V4 flow，按需要保留或禁用 metadata 记录。
   - retry: 基于表内 `flow_config` 与 `check_result` 支持幂等重试。
   - refresh: 定期重新执行 unify-query check，重新计算 `metricql`、`src_vm_table_ids` 与 flow config；如果和当前记录一致，则只更新检查时间，不刷新计算任务。

8. 定期检查刷新
   - 由于路由、存储、RT、VM 集群等配置可能变化，`check_result.data[].metricql` 和 `src_vm_table_ids` 都不是永久稳定值。
   - 新增周期任务扫描运行中的 V4 预计算规则，按原始 `input_type` / `input_config` 重新调用 check 接口。
   - 所有 `running` 规则都会被周期任务扫描。
   - 比对范围至少包括 `metricql`、`src_vm_table_ids`、目标 VM storage、生成后的 `flow_config`。
   - 比对无变化时，不调用 V4 apply / delete，不重启 flow，只更新 `last_check_time`。
   - 比对有变化且 `auto_refresh=true` 时，按更新策略刷新 V4 flow，并写回新的 `check_result`、`metricql`、`src_vm_table_ids`、`flow_config`、`last_refresh_time`，同时将 `has_change` 置为 `false`。
   - 比对有变化但 `auto_refresh=false` 时，不覆盖当前生效配置，不刷新 flow，只将 `has_change` 置为 `true`，表示存在可手动更新的状态。
   - check 失败时不应覆盖现有可运行配置，只记录 `last_error` 和 `trace_id`，等待下一轮重试或人工处理。

9. 对外查询集成
   - 将 V4 预计算输出 RT 纳入空间路由缓存。
   - 将输出指标补入指标列表能力。
   - 明确与现有 `get_record_rule_metrics_by_biz_id` 的兼容策略，是扩展函数还是新增 V4 查询函数。
