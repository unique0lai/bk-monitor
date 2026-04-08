# bk-monitor-base metric_plugin 域能力映射分析

## 1. 核心发现

**bk-monitor-base 的 `metric_plugin` 域已经包含了完整的「采集部署」管理能力。**

在 base 的建模中，采集配置被抽象为「插件部署项」（MetricPluginDeployment），采集下发/启停/重试/终止等操作都已有对应的 operation 函数和 installer 实现。这意味着 collecting 模块的适配核心不在于「补齐能力」，而在于「建立旧模型到新模型的兼容映射」。

## 2. 概念映射

### 2.1 数据模型映射

| 旧模型（collecting/plugin） | Base 模型（metric_plugin） | 说明 |
|---|---|---|
| `CollectConfigMeta` | `MetricPluginDeployment` / `MetricPluginDeploymentModel` | 采集配置 ↔ 插件部署项 |
| `DeploymentConfigVersion` | `MetricPluginDeploymentVersion` / `MetricPluginDeploymentVersionModel` | 部署配置版本 ↔ 部署版本 |
| `CollectorPluginMeta` | `MetricPlugin` / `MetricPluginModel` | 采集插件 ↔ 指标插件 |
| `PluginVersionHistory` | `MetricPluginVersionModel` | 插件版本历史 ↔ 插件版本 |
| 旧 `BaseInstaller`（deploy/base.py） | Base `BaseInstaller`（installer/base.py） | 安装器基类 |
| 旧 `NodeManInstaller` | Base `NodemanInstaller` | 节点管理安装器 |
| 旧 `K8sInstaller` | Base `K8sInstaller` | K8s 安装器 |
| `get_collect_installer()` | `get_installer()` | 安装器工厂 |

### 2.2 字段级映射

#### CollectConfigMeta → MetricPluginDeployment

| 旧字段 | Base 字段 | 转换说明 |
|--------|----------|---------|
| `id` | `id` | 直接映射 |
| `name` | `name` | 直接映射 |
| `bk_biz_id` | `bk_biz_id` | 直接映射 |
| `bk_tenant_id` | `bk_tenant_id` | 直接映射 |
| `plugin_id` | `plugin_id` | 直接映射 |
| `collect_type` | 通过 `plugin_id` 关联 `MetricPlugin.type` | 间接获取 |
| `target_object_type` | 通过 `MetricPlugin.label` 推导 | HOST/SERVICE 来自插件标签 |
| `last_operation` | 无直接对应 | Base 只记录 `status`，不记录操作类型 |
| `operation_result` | `status` | 状态枚举需映射 |
| `cache_data` | 无直接对应 | 需在 SaaS 层维护或 base 扩展 |
| `label` / `label_info` | 无直接对应 | 可通过 `MetricPlugin.label` 获取 |
| `create_time` / `update_time` | `created_at` / `updated_at` | 字段名格式差异 |
| `create_user` / `update_user` | `created_by` / `updated_by` | 字段名格式差异 |

#### DeploymentConfigVersion → MetricPluginDeploymentVersion

| 旧字段 | Base 字段 | 转换说明 |
|--------|----------|---------|
| `config_meta_id` | `deployment_id` | 外键关联 |
| `plugin_version` | `plugin_version: VersionTuple` | 旧版是 FK，base 用 VersionTuple |
| `target_node_type` | `target_scope.node_type` | 嵌套在 scope 对象中 |
| `target_nodes` | `target_scope.nodes` | 嵌套在 scope 对象中 |
| `params` | `params` | 直接映射 |
| `remote_collecting_host` | `remote_scope` | 结构差异：旧版单主机，base 用 scope |
| `subscription_id` | `deployment.related_params["subscription_id"]` | 存储位置不同 |
| `task_ids` | `deployment.related_params["task_ids"]` | 存储位置不同 |
| `last_version` (FK) | 通过 version 排序获取上一版本 | 查询方式不同 |

### 2.3 状态枚举映射

| 旧枚举（collecting/constant.py） | Base 枚举（MetricPluginDeploymentStatusEnum） | 说明 |
|---|---|---|
| `OperationResult.PREPARING` | `INITIALIZING` | 初始化 |
| `OperationResult.DEPLOYING` | `DEPLOYING` | 部署中 |
| `OperationResult.SUCCESS` | `RUNNING` | 运行中 |
| `OperationResult.FAILED` | `FAILED` | 失败 |
| `TaskStatus.STOPPED` | `STOPPED` | 已停止 |
| `TaskStatus.STARTING` | `STARTING` | 启动中 |
| `TaskStatus.STOPPING` | `STOPPING` | 停止中 |
| `OperationResult.WARNING` | 无直接对应 | Base 无部分失败状态，需 SaaS 层计算 |
| `Status.AUTO_DEPLOYING` | 无直接对应 | 自动下发状态需 SaaS 层处理 |

## 3. Operation 函数映射

### 3.1 核心 CRUD

| 旧 Resource | Base Operation 函数 | 适配要点 |
|---|---|---|
| `SaveCollectConfigResource` | `save_and_install_metric_plugin_deployment()` | 入参转换：data → `CreateOrUpdateDeploymentParams` |
| `CollectConfigListResource` | `list_metric_plugin_deployments()` | 旧版有更复杂的过滤/排序/缓存逻辑，base 只提供基础列表 |
| `CollectConfigDetailResource` | `get_metric_plugin_deployment()` | 返回 `(deployment, version)`，需组装为旧格式 |
| `DeleteCollectConfigResource` | `delete_metric_plugin_deployment()` | Base 要求先 stop 再 delete |
| `RenameCollectConfigResource` | 直接更新 `MetricPluginDeploymentModel.name` | base 无专门 rename API |

### 3.2 启停操作

| 旧 Resource | Base Operation 函数 | 适配要点 |
|---|---|---|
| `ToggleCollectConfigStatusResource(enable)` | `start_metric_plugin_deployment()` | 状态校验不同：base 要求 STOPPED → STARTING |
| `ToggleCollectConfigStatusResource(disable)` | `stop_metric_plugin_deployment()` | 状态校验不同：base 要求 RUNNING → STOPPING |

### 3.3 部署操作

| 旧 Resource | Base Operation 函数 | 适配要点 |
|---|---|---|
| `RetryTargetNodesResource` | `retry_metric_plugin_deployment()` | 入参差异：旧版 `instance_id`，base 用 `RetryDeployPluginParams.instance_scope` |
| `BatchRetryConfigResource` | `retry_metric_plugin_deployment()` | scope=None 表示重试全部 |
| `RevokeTargetNodesResource` | Base `installer.revoke(scope)` | 需通过 installer 调用 |
| `BatchRevokeTargetNodesResource` | Base `installer.revoke()` | scope=None 终止全部 |
| `RunCollectConfigResource` | Base `installer.run(action, scope)` | 参数结构需适配 |
| `UpgradeCollectPluginResource` | `save_and_install_metric_plugin_deployment()` | 升级 = 用新版本重新安装 |
| `RollbackDeploymentConfigResource` | **无直接对应** | 需通过获取上一版本参数重新 install |

### 3.4 状态查询

| 旧 Resource | Base Operation 函数 | 适配要点 |
|---|---|---|
| `CollectTargetStatusResource` | `get_metric_plugin_deployment_status()` | 返回格式需兼容 |
| `CollectRunningStatusResource` | `get_metric_plugin_deployment_status()` | diff=False 场景 |
| `CollectInstanceStatusResource` | `get_metric_plugin_deployment_status()` | diff=False 场景 |
| `GetCollectLogDetailResource` | `get_nodeman_collect_log_detail()` | 已有直接对应 |
| `GetMetricsResource` | `get_metric_plugin()` → `plugin.metrics` | 通过插件获取指标 |

### 3.5 工具类

| 旧 Resource | Base Operation 函数 | 适配要点 |
|---|---|---|
| `GetTrapCollectorPluginResource` | `create_metric_plugin()` | 虚拟插件创建走 base |
| `IsTaskReady` | `check_subscription_task_ready()` | nodeman API 已封装 |
| `CheckPluginVersionResource` | 无直接对应 | 保留 SaaS 层 |
| `EncryptPasswordResource` | 无直接对应 | 保留 SaaS 层 |
| 遗留订阅类 | 无直接对应 | 保留 SaaS 层 |

## 4. 安装器映射

### Base `BaseInstaller` 抽象方法

| 方法 | 旧安装器对应 | 说明 |
|------|------------|------|
| `install(deployment_version)` | `install(data, operation)` | 参数结构不同 |
| `uninstall()` | `uninstall()` | 一致 |
| `stop()` | `stop()` | 一致 |
| `start()` | `start()` | 一致 |
| `run(action, scope)` | `run(action, scope)` | scope 类型不同 |
| `retry(scope)` | `retry(instance_ids)` | 参数类型不同 |
| `revoke(scope)` | `revoke(instance_ids)` | 参数类型不同 |
| `status()` | `status(diff)` | Base 无 diff 参数 |
| - | `rollback()` | **Base 无 rollback** |
| - | `upgrade(params)` | **Base 通过 install 实现升级** |
| - | `instance_status(instance_id)` | NodemanInstaller 上有 |

### 关键差异

1. **Base 统一了 install 和 upgrade**：旧版分两个方法，base 统一通过 `install(new_deployment_version)` 实现
2. **Base 无 rollback**：回滚需在 SaaS 层通过「获取上一版本 → 重新 install」实现
3. **Base 无 diff 模式的 status**：旧版 `status(diff=True)` 会比对当前版本和上一版本的目标节点差异
4. **scope 替代 instance_ids**：Base 用 `MetricPluginDeploymentScope(node_type, nodes)` 替代 `list[str]`
5. **version_diff 内置**：Base `BaseInstaller.get_version_diff()` 提供了版本差异比对
6. **Base 新增 SQLInstaller / JobInstaller**：旧版没有 Job 类安装器，base 扩展了 SQL 类插件支持
7. **status() 返回值格式**：Base `NodemanInstaller.status()` 返回树状结构（与旧版格式高度一致），包含拓扑/动态分组/模板/主机实例的分组
8. **instance_status() 在 NodemanInstaller 上**：Base 在 NodemanInstaller 上有 `instance_status(instance_id)` → 返回日志详情

## 4.5 status() 实现差异深度对比

> ⚠️ **核心风险点：新旧 status() 存在架构级差异，不能简单视为格式兼容。**

### 4.5.1 最关键差异：diff 模式

旧版 `NodeManInstaller.status(diff=True/False)` 有一个 **`diff` 参数**，这是 `CollectTargetStatusResource` 的默认行为（`diff=True`）：

- **`diff=True`（默认）**：比较 `current_version` 与 `last_version`（上一部署版本），将目标节点分为 `ADD`/`REMOVE`/`UPDATE`/`RETRY` 四类。前端用这些分类展示「本次部署变更了什么」。
- **`diff=False`**：不做差异比对，展示当前全量节点状态。

**Base `NodemanInstaller.status()` 没有 `diff` 参数，也没有版本差异比对能力。** 它始终返回当前全量状态。

### 4.5.2 返回结构差异

#### 旧版 status() 返回结构（diff=True，TOPO 场景）

```python
# 按差异类型 + 拓扑路径分组
[
    {
        "child": [...实例列表...],
        "node_path": "蓝鲸/公共组件/kafka",  # 拓扑路径字符串
        "label_name": "ADD",                  # 差异类型 ADD/REMOVE/UPDATE/RETRY
        "is_label": True,                     # 是否有差异标签
    },
    {
        "child": [...],
        "node_path": "蓝鲸/公共组件/redis",
        "label_name": "RETRY",
        "is_label": True,
    },
]
```

#### 旧版 status() 返回结构（diff=True，HOST 场景）

```python
# 按差异类型分组，所有主机归到 "主机" 节点下
[
    {
        "child": [...所有 ADD 的主机实例...],
        "node_path": "主机",
        "label_name": "ADD",
        "is_label": True,
    },
    {
        "child": [...所有 RETRY 的主机实例...],
        "node_path": "主机",
        "label_name": "RETRY",
        "is_label": True,
    },
]
```

#### Base status() 返回结构（TOPO 场景）

```python
# 按拓扑节点分组，无差异标签
[
    {
        "child": [...实例列表...],
        "node_name": "蓝鲸/公共组件/kafka",  # 注意：字段名不同
        "node_type": "TOPO",
        "node_id": "module|1",
        "bk_obj_id": "module",
        "bk_inst_id": 1,
        "bk_inst_name": "kafka",
        # ❌ 无 label_name / is_label / diff_type
    },
]
```

#### Base status() 返回结构（HOST 场景）

```python
# 单一节点，无差异分类
[
    {
        "child": [...所有主机实例...],
        "node_name": "主机",
        "node_type": "HOST",
        "node_id": "host",
        # ❌ 无 label_name / is_label
    },
]
```

### 4.5.3 节点级字段差异

| 字段 | 旧版 | Base | 差异 |
|------|------|------|------|
| `node_path` | ✅ 拓扑路径字符串 | ❌ → `node_name`（相似但名称不同） | 字段名不同 |
| `label_name` | ✅ 差异类型（ADD/UPDATE/RETRY 等） | ❌ 不存在 | **Base 完全没有** |
| `is_label` | ✅ 差异标签开关 | ❌ 不存在 | **Base 完全没有** |
| `diff_type` | ✅ 内部差异类型 | ❌ 不存在 | **Base 完全没有** |
| `node_type` | ❌ 不存在 | ✅ TOPO/HOST/DYNAMIC_GROUP | Base 特有 |
| `node_id` | ❌ 不存在 | ✅ "module\|1" 等 | Base 特有 |
| `bk_obj_id` / `bk_inst_id` | ❌ 不存在 | ✅ 拓扑对象/实例 ID | Base 特有 |
| `dynamic_group_name` | ✅ 有（动态分组场景） | ❌ → 在 `bk_inst_name` 中 | 位置不同 |

### 4.5.4 实例级（child[]）字段差异

| 字段 | 旧版 | Base | 差异 |
|------|------|------|------|
| `instance_id` | ✅ | ✅ | 一致 |
| `ip` | ✅ | ✅ | 一致 |
| `bk_host_id` | ✅ | ✅ | 一致 |
| `bk_host_name` | ✅ | ✅ | 一致 |
| `bk_cloud_id` | ✅ | ✅ | 一致 |
| `bk_supplier_id` | ✅ | ✅ | 一致 |
| **`task_id`** | ✅ 数字 | ❌ 不存在 | **Base 未返回** |
| **`plugin_version`** | ✅ 版本字符串 | ❌ 不存在 | **Base 未返回** |
| `status` | ✅ 有状态转换逻辑 | ✅ 无状态转换 | **转换逻辑不同**（见下） |
| **`log`** | ✅ 阶段名（"步骤-子步骤"） | ✅ 错误日志文本 | **内容语义不同** |
| `action` | ✅ install/uninstall/update | ✅ 同 | 一致 |
| `steps` | ✅ | ✅ | 一致 |
| `instance_name` | ✅ | ✅ | 一致 |
| `scope_ids` | ✅ 内部使用后删除 | ❌ → `related_node_ids` | 名称不同，保留策略不同 |
| `service_instance_id` | ✅ | ✅ | 一致 |
| `bk_module_id` / `bk_module_ids` | ✅ | ✅ | 一致 |

### 4.5.5 状态转换逻辑差异

**旧版**：根据 `collect_config.last_operation` 将中间状态转换为更具语义的状态：

```python
# 旧版 _process_nodeman_task_result()
if instance["status"] in [TaskStatus.DEPLOYING, TaskStatus.RUNNING]:
    instance["status"] = self.running_status.get(
        self.collect_config.last_operation, TaskStatus.RUNNING
    )
# running_status = {
#     OperationType.START: TaskStatus.STARTING,
#     OperationType.STOP: TaskStatus.STOPPING,
# }
```

即：如果最后操作是 START，则 RUNNING 状态显示为 STARTING；如果是 STOP，则显示为 STOPPING。

**Base**：**不做实例级状态转换**，但在方法末尾做 **deployment 整体状态更新**（根据所有实例的状态汇总判断 deployment 应该进入什么状态）。

### 4.5.6 拓扑解析方式差异

| 维度 | 旧版 | Base |
|------|------|------|
| 拓扑树获取 | `api.cmdb.get_topo_tree()` → `TopoTree` 对象 | `cmdb_api.find_topo_node_path()` |
| 路径计算 | `convert_to_topo_link()` 构建 module→biz 的完整链路 | 直接使用 API 返回的 `bk_paths` |
| 模板→节点转换 | `api.cmdb.get_module/get_set()` | `cmdb_api.list_set_template/list_service_template()` |
| 遗留 IP 兼容 | ✅ 有 IP→bk_host_id 转换逻辑 | ❌ 无（假设都用 bk_host_id） |

### 4.5.7 API 调用差异

| 旧版 | Base |
|------|------|
| `api.node_man.batch_task_result(subscription_id, task_id_list, need_detail)` | `batch_get_subscription_task_result(bk_tenant_id, params={subscription_id, need_detail, need_aggregate_all_tasks})` |
| 不检查 task ready | `check_subscription_task_ready()` 预检查，未 ready 时强制设置 PENDING |
| 按 `task_id_list` 查询 | 用 `need_aggregate_all_tasks=True` 聚合全部任务 |

### 4.5.8 适配影响评估

| 影响范围 | 严重程度 | 说明 |
|---------|---------|------|
| `CollectTargetStatusResource`（diff=True） | 🔴 高 | **diff 模式完全缺失**，需 SaaS 层实现版本差异比对+节点分类 |
| `CollectRunningStatusResource`（diff=False） | 🟡 中 | 无 diff 依赖，但节点分组结构和字段名需转换 |
| `CollectInstanceStatusResource`（diff=False） | 🟡 中 | 同上 |
| `CollectTargetStatusTopoResource` | 🟡 中 | 消费 `status(diff=False)` 的结果，但需要 `bk_module_id`/`bk_module_ids` |
| `UpdateConfigInstanceCountResource` | 🟢 低 | 仅统计 error/total，与分组结构无关 |
| 前端 `FrontendTargetStatusTopoResource` | 🟡 中 | 间接依赖 status 的结构 |

### 4.5.9 解决方案建议

#### 方案 A：在 SaaS compat 层包装 diff 逻辑

```python
def status_with_diff(deployment_id, bk_tenant_id, bk_biz_id, diff=True):
    """在 SaaS 层模拟旧版 status(diff) 行为"""

    # 1. 获取 base 的原始 status（无 diff）
    raw_status = get_metric_plugin_deployment_status(
        bk_tenant_id=bk_tenant_id,
        deployment_id=deployment_id,
        bk_biz_id=bk_biz_id,
    )

    # 2. 转换 base 节点结构 → 旧版结构
    instances = _extract_instances_from_base_status(raw_status)

    if not diff:
        # 非 diff 模式：直接按旧格式重组
        return _regroup_by_old_format(instances, node_type, target_nodes)

    # 3. diff 模式：获取上一版本的目标节点，执行差异比对
    deployment, current_version = get_metric_plugin_deployment(...)
    previous_version = _get_previous_version(deployment_id)
    node_diff = _compute_node_diff(current_version, previous_version)

    # 4. 将实例按差异类型分组
    return _group_instances_by_diff(instances, node_diff, node_type)
```

#### 方案 B：扩展 base installer.status() 支持 diff

在 `BaseInstaller.status()` 添加可选的 `diff` 参数和版本比对逻辑。但这会增加 base 复杂度，且 diff 逻辑本身偏 SaaS 展示层。

**推荐方案 A**：diff 属于展示层逻辑，保留在 SaaS 的 compat 层。

## 5. 入参转换设计（compat 层核心）

### 5.1 SaveCollectConfig → CreateOrUpdateDeploymentParams

```python
# 旧请求数据
old_data = {
    "id": 123,                          # → params.id
    "name": "my_collect",               # → params.name
    "bk_biz_id": 2,                     # → bk_biz_id
    "plugin_id": "my_plugin",           # → params.plugin_id
    "target_node_type": "TOPO",         # → params.target_scope.node_type
    "target_nodes": [...],              # → params.target_scope.nodes
    "params": {"collector": {}, "plugin": {}},  # → params.params
    "remote_collecting_host": {...},    # → params.remote_scope
}

# Base 参数
base_params = CreateOrUpdateDeploymentParams(
    id=old_data.get("id"),
    name=old_data["name"],
    plugin_id=old_data["plugin_id"],
    plugin_version=VersionTuple(major=..., minor=...),  # 需从插件当前版本获取
    target_scope=MetricPluginDeploymentScope(
        node_type=old_data["target_node_type"],
        nodes=old_data["target_nodes"],
    ),
    remote_scope=convert_remote_host_to_scope(old_data.get("remote_collecting_host")),
    params=old_data["params"],
)
```

### 5.2 出参转换：MetricPluginDeployment → 旧 CollectConfig 格式

```python
def convert_deployment_to_legacy(
    deployment: MetricPluginDeployment,
    version: MetricPluginDeploymentVersion,
    plugin: MetricPlugin,
) -> dict:
    return {
        "id": deployment.id,
        "deployment_id": deployment.id,   # 旧版有单独的 deployment_config_id
        "name": deployment.name,
        "bk_biz_id": deployment.bk_biz_id,
        "collect_type": plugin.type,
        "plugin_id": deployment.plugin_id,
        "target_object_type": label_to_object_type(plugin.label),
        "target_node_type": version.target_scope.node_type,
        "target_nodes": version.target_scope.nodes,
        "params": version.params,
        "remote_collecting_host": convert_scope_to_remote_host(version.remote_scope),
        "plugin_info": convert_plugin_to_legacy_info(plugin),
        "subscription_id": deployment.related_params.get("subscription_id", 0),
        "label": plugin.label,
        "label_info": {...},
        "create_time": deployment.created_at,
        "create_user": deployment.created_by,
        "update_time": deployment.updated_at,
        "update_user": deployment.updated_by,
    }
```

### 5.3 列表出参补充

旧版 `CollectConfigListResource` 返回的列表项包含一些 Base 不直接提供的字段，需在 SaaS 层补充：

| 字段 | 补充方式 |
|------|---------|
| `space_name` | 调 SpaceApi |
| `status` / `task_status` | 从 `deployment.status` 映射 |
| `need_upgrade` | 比较 deployment.plugin_version 与 plugin 最新 release_version |
| `config_version` / `info_version` | 从 plugin_version 拆解 |
| `error_instance_count` / `total_instance_count` | 调 `get_metric_plugin_deployment_status()` 或缓存 |
| `running_tasks` | 从 `related_params` 获取 |

## 6. 需在 SaaS 层补充的逻辑

### 6.1 无法由 Base 直接满足的能力

| 能力 | 原因 | 解决方案 |
|------|------|---------|
| `cache_data` 缓存机制 | Base 无此概念 | 在 SaaS 层维护缓存表或字段 |
| `allow_rollback` 计算 | Base 无回滚概念 | SaaS 层根据版本历史判断 |
| Rollback 操作 | Base 无 rollback 方法 | SaaS 层获取上一版本参数，调 `save_and_install` |
| 空间/业务权限过滤 | Base 未集成 Space | SaaS 层做过滤 |
| 告警策略清理 | monitoring 域耦合 | 保留 SaaS 层 |
| NoData 检测 | 数据查询层耦合 | 保留 SaaS 层 |
| 指标缓存刷新 | SaaS 异步任务 | 保留 SaaS 层 |
| 遗留订阅管理 | 运维工具 | 保留 SaaS 层 |

### 6.2 虚拟插件处理

旧版 `SaveCollectConfigResource.get_collector_plugin()` 针对 LOG/PROCESS/SNMP_TRAP/K8S 类型创建虚拟插件。适配时需要：

1. **LOG 类型**：使用 base `create_metric_plugin()` + 对应 manager
2. **PROCESS 类型**：使用 base `create_metric_plugin()` 替代 `PluginManagerFactory`
3. **SNMP_TRAP 类型**：base 已有 `SNMPTrapPluginManager`
4. **K8S 类型**：base 已有 `K8sInstaller`

## 7. 修订后的适配复杂度评估

Base 已有完整的部署管理能力，大部分 CRUD/操作类 Resource 复杂度大幅降低；
但 **status 类 Resource 因新旧结构差异大（diff 模式缺失、节点分组/实例字段不同）而维持较高复杂度**。

| Resource | 原评估 | 修订评估 | 修订原因 |
|----------|--------|---------|---------|
| SaveCollectConfigResource | 🔴 高 | 🟡 中 | 核心是入参转换 + 虚拟插件处理 |
| CollectConfigListResource | 🔴 高 | 🟡 中 | 基础列表 base 已有，缓存/状态补充在 SaaS 层 |
| CollectConfigDetailResource | 🟡 中 | 🟢 低 | `get_metric_plugin_deployment()` 直接可用 |
| ToggleCollectConfigStatusResource | 🟡 中 | 🟢 低 | `start/stop_metric_plugin_deployment()` 直接可用 |
| DeleteCollectConfigResource | 🟡 中 | 🟢 低 | `delete_metric_plugin_deployment()` 直接可用 |
| RetryTargetNodesResource | 🟢 低 | 🟢 低 | `retry_metric_plugin_deployment()` 直接可用 |
| **CollectTargetStatusResource** | 🟡 中 | **🔴 高** | **diff 模式在 base 完全缺失**，需 SaaS 层实现版本差异比对+节点分类+字段转换 |
| CollectRunningStatusResource | 🟢 低 | 🟡 中 | diff=False 但节点结构 + 实例字段仍需转换 |
| CollectInstanceStatusResource | 🟢 低 | 🟡 中 | 同 CollectRunningStatusResource |
| GetCollectLogDetailResource | 🟢 低 | 🟢 低 | `get_nodeman_collect_log_detail()` 直接可用 |
| RollbackDeploymentConfigResource | 🟡 中 | 🟡 中 | 需 SaaS 层模拟（获取上一版本重新 install） |
| CollectTargetStatusTopoResource | 🔴 高 | 🟡 中 | 消费 status(diff=False) + SaaS NoData 检测 |
| UpgradeCollectPluginResource | 🟡 中 | 🟢 低 | 通过 `save_and_install` 用新版本重新安装 |
