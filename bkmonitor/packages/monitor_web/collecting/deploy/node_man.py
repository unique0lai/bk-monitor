"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2024 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""
import copy
import itertools
from collections import defaultdict
from typing import Any, Dict, List, Union

from django.conf import settings
from django.utils.translation import ugettext as _

from api.cmdb.define import TopoNode, TopoTree
from constants.cmdb import TargetNodeType, TargetObjectType
from core.drf_resource import api
from core.errors.collecting import (
    CollectConfigNeedUpgrade,
    CollectConfigRollbackError,
    DeleteCollectConfigError,
    ToggleConfigStatusError,
)
from monitor_web.collecting.constant import (
    CollectStatus,
    OperationResult,
    OperationType,
)
from monitor_web.models import CollectConfigMeta, DeploymentConfigVersion
from monitor_web.plugin.constant import ParamMode, PluginType
from monitor_web.plugin.manager import PluginManagerFactory

from .base import BaseInstaller


class NodeManInstaller(BaseInstaller):
    """
    节点管理安装器
    """

    def __init__(self, collect_config: CollectConfigMeta, topo_tree: TopoTree = None):
        super().__init__(collect_config)
        self._topo_tree = topo_tree
        self._topo_links = None

    def _get_topo_links(self) -> Dict[str, List[TopoNode]]:
        """
        获取拓扑链路
        """
        if self._topo_links:
            return self._topo_links

        if not self._topo_tree:
            self._topo_tree = api.cmdb.get_topo_tree(bk_biz_id=self.collect_config.bk_biz_id)

        self._topo_links = self._topo_tree.convert_to_topo_link()
        return self._topo_links

    def _create_plugin_collecting_steps(self, target_version: DeploymentConfigVersion, data_id: str):
        """
        创建插件采集步骤配置
        """
        plugin_manager = PluginManagerFactory.get_manager(plugin=self.plugin)
        config_params = copy.deepcopy(target_version.params)

        # 获取维度注入参数
        config_json = target_version.plugin_version.config.config_json
        dms_insert_params = {}
        for param in config_json:
            if param["mode"] == ParamMode.DMS_INSERT:
                param_value = config_params["plugin"].get(param['name'])
                for dms_key, dms_value in list(param_value.items()):
                    if param["type"] == "host":
                        dms_insert_params[dms_key] = "{{ " + f"cmdb_instance.host.{dms_value} or '-'" + " }}"
                    else:
                        dms_insert_params[dms_key] = (
                            "{{ " + f"cmdb_instance.service.labels['{dms_value}'] or '-'" + " }}"
                        )

        if self.plugin.plugin_type == PluginType.PROCESS:
            # processbeat 配置
            # processbeat 采集不需要dataid
            config_params["collector"].update(
                {
                    "taskid": str(self.collect_config.id),
                    "namespace": self.plugin.plugin_id,
                    # 采集周期带上单位 `s`
                    "period": f"{config_params['collector']['period']}s",
                    # 采集超时时间
                    "timeout": f"{config_params['collector'].get('timeout', 60)}",
                    "max_timeout": f"{config_params['collector'].get('timeout', 60)}",
                    "dataid": str(plugin_manager.perf_data_id),
                    "port_dataid": str(plugin_manager.port_data_id),
                    "match_pattern": config_params["process"]["match_pattern"],
                    "process_name": config_params["process"].get("process_name", ""),
                    "exclude_pattern": config_params["process"]["exclude_pattern"],
                    "port_detect": config_params["process"]["port_detect"],
                    # 维度注入能力
                    "extract_pattern": config_params["process"].get("extract_pattern", ""),
                    "pid_path": config_params["process"]["pid_path"],
                    "labels": {
                        "$for": "cmdb_instance.scope",
                        "$item": "scope",
                        "$body": {
                            "bk_target_host_id": "{{ cmdb_instance.host.bk_host_id }}",
                            "bk_target_ip": "{{ cmdb_instance.host.bk_host_innerip }}",
                            "bk_target_cloud_id": (
                                "{{ cmdb_instance.host.bk_cloud_id[0].id "
                                "if cmdb_instance.host.bk_cloud_id is iterable and "
                                "cmdb_instance.host.bk_cloud_id is not string "
                                "else cmdb_instance.host.bk_cloud_id }}"
                            ),
                            "bk_target_topo_level": "{{ scope.bk_obj_id }}",
                            "bk_target_topo_id": "{{ scope.bk_inst_id }}",
                            "bk_target_service_category_id": (
                                "{{ cmdb_instance.service.service_category_id | default('', true) }}"
                            ),
                            "bk_collect_config_id": self.collect_config.id,
                            "bk_biz_id": str(self.collect_config.bk_biz_id),
                        },
                    },
                    "tags": config_params["collector"].get("tag", {}),
                }
            )
        else:
            # bkmonitorbeat通用配置参数
            config_params["collector"].update(
                {
                    "task_id": str(self.collect_config.id),
                    "bk_biz_id": str(self.collect_config.bk_biz_id),
                    "config_name": self.plugin.plugin_id,
                    "config_version": "1.0",
                    "namespace": self.plugin.plugin_id,
                    "period": str(config_params["collector"]["period"]),
                    # 采集超时时间
                    "timeout": f"{config_params['collector'].get('timeout', 60)}",
                    "max_timeout": f"{config_params['collector'].get('timeout', 60)}",
                    "dataid": str(data_id),
                    "labels": {
                        "$for": "cmdb_instance.scope",
                        "$item": "scope",
                        "$body": {
                            "bk_target_host_id": "{{ cmdb_instance.host.bk_host_id }}",
                            "bk_target_ip": "{{ cmdb_instance.host.bk_host_innerip }}",
                            "bk_target_cloud_id": (
                                "{{ cmdb_instance.host.bk_cloud_id[0].id "
                                "if cmdb_instance.host.bk_cloud_id is iterable and "
                                "cmdb_instance.host.bk_cloud_id is not string "
                                "else cmdb_instance.host.bk_cloud_id }}"
                            ),
                            "bk_target_topo_level": "{{ scope.bk_obj_id }}",
                            "bk_target_topo_id": "{{ scope.bk_inst_id }}",
                            "bk_target_service_category_id": (
                                "{{ cmdb_instance.service.service_category_id | default('', true) }}"
                            ),
                            "bk_target_service_instance_id": "{{ cmdb_instance.service.id }}",
                            "bk_collect_config_id": self.collect_config.id,
                            # 维度注入模板变量
                            **dms_insert_params,
                        },
                    },
                }
            )
        config_params["subscription_id"] = target_version.subscription_id
        return plugin_manager.get_deploy_steps_params(
            target_version.plugin_version, config_params, target_version.target_nodes
        )

    def _get_deploy_params(self, target_version: DeploymentConfigVersion):
        """
        获取订阅任务参数
        """
        data_id = self.collect_config.data_id

        subscription_params = {
            "scope": {
                "bk_biz_id": self.collect_config.bk_biz_id,
                "object_type": self.collect_config.target_object_type,
                "node_type": target_version.target_node_type,
                "nodes": [target_version.remote_collecting_host]
                if self.plugin.plugin_type == PluginType.SNMP
                else target_version.target_nodes,
            },
            "steps": self._create_plugin_collecting_steps(target_version, data_id),
            "run_immediately": True,
        }

        # 在组装节点管理创建订阅时，target_hosts被定义为远程下发采集配置文件与执行采集任务的主机
        if target_version.remote_collecting_host:
            if self.plugin.plugin_type == PluginType.SNMP:
                return subscription_params
            subscription_params["target_hosts"] = [target_version.remote_collecting_host]

        return subscription_params

    def _deploy(self, target_version: DeploymentConfigVersion):
        """
        部署插件采集
        """
        last_version = self.collect_config.deployment_config

        # 判断是否需要重建订阅任务
        if last_version and last_version.subscription_id:
            diff_result = last_version.show_diff(target_version)
            operate_type = self.collect_config.operate_type(diff_result)
        else:
            operate_type = "create"

        subscription_params = self._get_deploy_params(target_version)
        if operate_type == "create":
            # 新建订阅任务
            result = api.node_man.create_subscription(**subscription_params)
            subscription_id = result["subscription_id"]
            task_id = result["task_id"]
        elif operate_type == "update":
            # 更新上一次订阅任务
            update_params = {
                "subscription_id": last_version.subscription_id,
                "scope": {
                    "bk_biz_id": self.collect_config.bk_biz_id,
                    "node_type": subscription_params["scope"]["node_type"],
                    "nodes": subscription_params["scope"]["nodes"],
                },
                "steps": subscription_params.get("steps", []),
                "run_immediately": True,
            }
            result = api.node_man.update_subscription(**update_params)
            subscription_id = last_version.subscription_id
            task_id = result["task_id"]
        else:
            # 新建订阅任务
            result = api.node_man.create_subscription(**subscription_params)
            subscription_id = result["subscription_id"]
            task_id = result["task_id"]

            # 卸载上一次订阅任务
            api.node_man.run_subscription(
                subscription_id=last_version.subscription_id,
                actions=[{step["id"]: "UNINSTALL"} for step in subscription_params["steps"]],
            )
            api.node_man.delete_subscription(subscription_id=last_version.subscription_id)

        # 启动自动巡检
        if settings.IS_SUBSCRIPTION_ENABLED:
            api.node_man.switch_subscription(subscription_id=subscription_id, action="enable")

        # 更新部署记录及采集配置
        target_version.subscription_id = subscription_id
        target_version.task_ids = [task_id]
        target_version.save()

    def install(self, install_config: Dict):
        """
        首次安装插件采集
        """
        # 判断该采集是否需要升级，如果需要升级则抛出异常
        if self.collect_config.pk and self.collect_config.need_upgrade:
            raise CollectConfigNeedUpgrade({"msg": self.collect_config.name})

        # 创建新的部署记录
        deployment_config_params = {
            "plugin_version": self.plugin.packaged_release_version,
            "target_node_type": install_config["target_node_type"],
            "target_nodes": install_config["target_nodes"],
            "params": install_config["params"],
            "remote_collecting_host": install_config.get("remote_collecting_host"),
            "config_meta_id": self.collect_config.pk or 0,
            "parent_id": self.collect_config.deployment_config.pk if self.collect_config.deployment_config else 0,
        }
        new_version = DeploymentConfigVersion.objects.create(**deployment_config_params)

        # 部署插件采集
        self._deploy(new_version)

        # 更新采集配置
        self.collect_config.operation_result = OperationResult.PREPARING
        self.collect_config.last_operation = OperationType.EDIT if self.collect_config.pk else OperationType.CREATE
        self.collect_config.deployment_config = new_version
        self.collect_config.save()

        # 如果是首次创建，更新部署配置关联的采集配置ID
        if not new_version.config_meta_id:
            new_version.config_meta_id = self.collect_config.pk
            new_version.save()

    def uninstall(self):
        """
        卸载插件采集
        1. 判断是否已经停用
        2. 删除节点管理订阅任务
        """
        # 判断是否已经停用
        if self.collect_config.last_operation != OperationType.STOP:
            raise DeleteCollectConfigError({"msg": _("采集配置未停用")})

        # 删除节点管理订阅任务
        subscription_id = self.collect_config.deployment_config.subscription_id
        subscription_params = self._get_deploy_params(self.collect_config.deployment_config)
        api.node_man.run_subscription(
            subscription_id=subscription_id,
            actions=[{step["id"]: "UNINSTALL"} for step in subscription_params["steps"]],
        )
        api.node_man.delete_subscription(subscription_id=subscription_id)

        # 删除部署记录及采集配置
        DeploymentConfigVersion.objects.filter(config_meta_id=self.collect_config.id).delete()
        self.collect_config.delete()

    def rollback(self, target_version: Union[int, DeploymentConfigVersion, None] = None):
        """
        回滚插件采集
        """
        # 判断是否支持回滚
        if not self.collect_config.allow_rollback:
            raise CollectConfigRollbackError({"msg": _("当前操作不支持回滚，或采集配置正处于执行中")})

        # 获取目标版本
        if not target_version:
            target_version = self.collect_config.deployment_config.last_version
        elif isinstance(target_version, int):
            target_version = DeploymentConfigVersion.objects.get(pk=target_version)

        # 回滚部署
        self._deploy(target_version)

        # 更新采集配置
        self.collect_config.deployment_config = target_version
        self.collect_config.operation_result = OperationResult.PREPARING
        self.collect_config.last_operation = OperationType.ROLLBACK
        self.collect_config.save()

    def stop(self):
        """
        停止插件采集
        1. 关闭订阅任务巡检
        2. 执行停止操作
        """
        if self.collect_config.last_operation == OperationType.STOP:
            raise ToggleConfigStatusError({"msg": _("采集配置已处于停用状态，无需重复执行停止操作")})

        subscription_id = self.collect_config.deployment_config.subscription_id

        # 关闭订阅任务巡检
        api.node_man.switch_subscription(subscription_id=subscription_id, action="disable")

        # 停用采集配置
        subscription_params = self._get_deploy_params(self.collect_config.deployment_config)
        result = api.node_man.run_subscription(
            subscription_id=subscription_id,
            actions=[{step["id"]: "STOP"} for step in subscription_params["steps"]],
        )

        # 更新采集配置及部署记录
        self.collect_config.operation_result = OperationResult.PREPARING
        self.collect_config.last_operation = OperationType.STOP
        self.collect_config.save()

        self.collect_config.deployment_config.task_ids = [result["task_id"]]
        self.collect_config.deployment_config.save()

    def start(self):
        """
        启动插件采集
        1. 启动订阅任务巡检
        2. 执行启动操作
        """
        if self.collect_config.last_operation != OperationType.STOP:
            raise ToggleConfigStatusError({"msg": _("采集配置未处于停用状态，无法执行启动操作")})

        subscription_id = self.collect_config.deployment_config.subscription_id

        # 启用订阅任务巡检
        if settings.IS_SUBSCRIPTION_ENABLED:
            api.node_man.switch_subscription(subscription_id=subscription_id, action="enable")

        # 启动采集配置
        subscription_params = self._get_deploy_params(self.collect_config.deployment_config)
        result = api.node_man.run_subscription(
            subscription_id=subscription_id,
            actions=[{step["id"]: "START"} for step in subscription_params["steps"]],
        )

        # 更新采集配置及部署记录
        self.collect_config.operation_result = OperationResult.PREPARING
        self.collect_config.last_operation = OperationType.START
        self.collect_config.save()

        self.collect_config.deployment_config.task_ids = [result["task_id"]]
        self.collect_config.deployment_config.save()

    def retry(self, instance_ids: List[int] = None):
        """
        重试插件采集
        """
        subscription_id = self.collect_config.deployment_config.subscription_id

        # 如果没有指定实例ID，则重试所有实例
        params = {"subscription_id": subscription_id}
        if instance_ids is not None:
            params["instance_id_list"] = instance_ids

        # 重试订阅任务
        result = api.node_man.retry_subscription(**params)

        # 更新采集配置及部署记录
        self.collect_config.deployment_config.task_ids.append(result["task_id"])
        self.collect_config.deployment_config.save()
        self.collect_config.operation_result = OperationResult.PREPARING
        self.collect_config.save()

    def revoke(self, instance_ids: List[int] = None):
        """
        终止采集任务
        """
        subscription_id = self.collect_config.deployment_config.subscription_id

        # 如果没有指定实例ID，则终止所有实例
        params = {"subscription_id": subscription_id}
        if instance_ids is not None:
            params["instance_id_list"] = instance_ids

        # 终止订阅任务
        api.node_man.revoke_subscription(**params)

    @staticmethod
    def _get_instance_step_log(instance_result: Dict[str, Any]):
        """
        获取实例下发阶段性日志
        """
        for step in instance_result.get("steps", []):
            if step["status"] != CollectStatus.SUCCESS:
                for sub_step in step["target_hosts"][0]["sub_steps"]:
                    if sub_step["status"] != CollectStatus.SUCCESS:
                        return "{}-{}".format(step["node_name"], sub_step["node_name"])
        return ""

    def _process_nodeman_task_result(self, task_result: List[Dict[str, Any]]):
        """
        处理节点管理任务结果
        {
          "task_id":1,
          "record_id":1,
          "instance_id":"service|instance|service|1",
          "create_time":"2024-09-06 12:07:33",
          "pipeline_id":"xxxxxxxxxxxx",
          "instance_info":{
            "host":{
              "bk_biz_id":2,
              "bk_host_id":1,
              "bk_biz_name":"蓝鲸",
              "bk_cloud_id":0,
              "bk_host_name":"VM_0_0_centos",
              "bk_cloud_name":"云区域",
              "bk_host_innerip":"127.0.0.1",
              "bk_supplier_account":"0"
            },
            "service":{
              "id":4324,
              "name":"127.0.0.1_mysql_3306",
              "bk_host_id":1,
              "bk_module_id":1
            }
          },
          "start_time":"2024-09-06 12:07:35",
          "finish_time":"2024-09-06 12:07:54",
          "status":"SUCCESS"
        }

        {
          "task_id":1,
          "record_id":1,
          "instance_id":"host|instance|host|1",
          "create_time":"2024-09-06 12:40:17",
          "pipeline_id":"xxxxxxxxxxxx",
          "instance_info":{
            "host":{
              "bk_biz_id":2,
              "bk_host_id":1,
              "bk_biz_name":"蓝鲸",
              "bk_cloud_id":0,
              "bk_host_name":"VM_0_0_centos",
              "bk_cloud_name":"云区域",
              "bk_host_innerip":"127.0.0.1",
              "bk_host_innerip_v6":"",
              "bk_supplier_account":"0"
            },
            "service":{}
          },
          "start_time":"2024-09-06 12:40:19",
          "finish_time":"2024-09-06 12:40:27",
          "status":"SUCCESS"
        }
        """
        instances = []
        for instance_result in task_result:
            host = instance_result["instance_info"]["host"]
            service_info = instance_result["instance_info"].get("service") or {}
            instance = {
                "instance_id": instance_result["instance_id"],
                "ip": host["bk_host_innerip"],
                "bk_cloud_id": host["bk_cloud_id"],
                "bk_host_id": host["bk_host_id"],
                "bk_host_name": host["bk_host_name"],
                "bk_supplier_id": host["bk_supplier_account"],
                "task_id": instance_result["task_id"],
                "status": instance_result["status"],
                "plugin_version": self.collect_config.deployment_config.plugin_version.version,
                "log": self._get_instance_step_log(instance_result),
                "action": "",
                "steps": {step["id"]: step["action"] for step in instance_result.get("steps", []) if step["action"]},
                "scope_ids": [],
            }

            # 处理scope
            for scope in instance_result["instance_info"].get("scope", []):
                if "ip" in scope:
                    instance["scope_ids"].append(host["bk_host_id"])
                elif "bk_obj_id" in scope and "bk_inst_id" in scope:
                    instance["scope_ids"].append(f"{scope['bk_obj_id']}|{scope['bk_inst_id']}")

            # 处理服务实例与主机差异字段
            if instance["instance_id"].startswith("service|instance"):
                instance.update(
                    {
                        "instance_name": service_info.get("name") or service_info["id"],
                        "service_instance_id": service_info["id"],
                        "bk_module_id": service_info["bk_module_id"],
                    }
                )
            else:
                instance["instance_name"] = host.get("bk_host_innerip") or host.get("bk_host_innerip_v6") or ""

            # 根据步骤获取操作类型
            action = "install"
            for step in instance_result.get("steps", []):
                if step.get("action") in ["UNINSTALL", "REMOVE_CONFIG"]:
                    action = "uninstall"
                elif step.get("action") in ["INSTALL"]:
                    action = "install"
                elif step.get("action") in ["PUSH_CONFIG"]:
                    action = "update"

            instance["action"] = action
            instances.append(instance)

        return instances

    def status(self, diff=False) -> List[Dict[str, Any]]:
        """
        状态查询
        :param diff: 是否显示差异
        """
        # 获取订阅任务状态，并将结果转换为需要的数据结构
        subscription_id = self.collect_config.deployment_config.subscription_id
        if not subscription_id:
            return []
        result = api.node_man.batch_task_result(subscription_id=subscription_id, need_detail=True)
        instance_statuses = self._process_nodeman_task_result(result)

        # 差异比对/不比对数据结构
        current_version = self.collect_config.deployment_config
        last_version = current_version.last_version

        # 将模板转换为节点
        template_to_nodes = defaultdict(list)
        current_node_type = current_version.target_node_type
        if (
            current_node_type in [TargetNodeType.SERVICE_TEMPLATE, TargetNodeType.SET_TEMPLATE]
            and current_version.target_nodes
        ):
            opt_mapping = {
                TargetNodeType.SERVICE_TEMPLATE: {"field": "service_template_id", "api": api.cmdb.get_module},
                TargetNodeType.SET_TEMPLATE: {"field": "set_template_id", "api": api.cmdb.get_set},
            }

            template_ids = [node["bk_inst_id"] for node in current_version.target_nodes]
            topo_nodes = opt_mapping[current_node_type]["api"](
                bk_biz_id=self.collect_config.bk_biz_id, **{f'{opt_mapping[current_node_type]["field"]}s': template_ids}
            )
            for node in topo_nodes:
                template_id = getattr(node, opt_mapping[current_node_type]["field"])
                template_to_nodes[f"{current_node_type}|{template_id}"].append(
                    {"bk_obj_id": node.bk_obj_id, "bk_inst_id": node.bk_inst_id}
                )

        # 老主机配置兼容，将ip/bk_cloud_id转换为bk_host_id，方便后续数据处理
        if (
            current_node_type == TargetNodeType.INSTANCE
            and self.collect_config.target_object_type == TargetObjectType.HOST
        ):
            # 统计旧版主机配置
            ips = []
            for host in itertools.chain(
                current_version.target_nodes, last_version.target_nodes if last_version else []
            ):
                if "bk_host_id" in host:
                    continue
                ips.append({"ip": host["ip"], "bk_cloud_id": host.get("bk_cloud_id", 0)})

            # 查询主机信息
            hosts = api.cmdb.get_host_by_ip(bk_biz_id=self.collect_config.bk_biz_id, ips=ips)
            ip_to_host_ids = {f"{host.bk_host_innerip}|{host.bk_cloud_id}": host.bk_host_id for host in hosts}

            # 标记主机ID
            for host in itertools.chain(
                current_version.target_nodes, last_version.target_nodes if last_version else []
            ):
                if f"{host['ip']}|{host.get('bk_cloud_id', 0)}" in ip_to_host_ids:
                    host["bk_host_id"] = ip_to_host_ids[f"{host['ip']}|{host.get('bk_cloud_id', 0)}"]

            # 过滤主机ID为空的节点
            current_version.target_nodes = [
                {"bk_host_id": host["bk_host_id"]} for host in current_version.target_nodes if "bk_host_id" in host
            ]
            if last_version:
                last_version.target_nodes = [
                    {"bk_host_id": host["bk_host_id"]} for host in last_version.target_nodes if "bk_host_id" in host
                ]

        # 差异比对
        if diff and last_version:
            # 如果存在上一个版本，且需要显示差异
            # {
            #     "is_modified": true,
            #     "added": [],
            #     "updated": [],
            #     "removed": [{"bk_host_id": 51985}],
            #     "unchanged": [{"bk_host_id": 96886},{"bk_host_id": 96887}]
            # }
            node_diff = last_version.show_diff(current_version)["nodes"]

            # removed目前不会显示出来，如果后续需要显示，还需要处理父节点的数据，通知节点管理也需要查询目标范围外的任务结果
            node_diff.pop("removed", None)

            # 如果没有差异，直接返回
            if not node_diff.pop("is_modified", True):
                return []
        else:
            node_diff = {"": current_version.target_nodes}

        nodes = {}
        for diff_type, diff_nodes in node_diff.items():
            for diff_node in diff_nodes:
                # 主机节点
                if "bk_host_id" in diff_node:
                    nodes[diff_node["bk_host_id"]] = {"diff_type": diff_type, "child": []}
                    continue

                # 将服务/集群模板转换为拓扑节点
                if f"{diff_node['bk_obj_id']}|{diff_node['bk_inst_id']}" in template_to_nodes:
                    template_nodes = template_to_nodes[f"{diff_node['bk_obj_id']}|{diff_node['bk_inst_id']}"]
                else:
                    template_nodes = [diff_node]

                for node in template_nodes:
                    nodes[f"{node['bk_obj_id']}|{node['bk_inst_id']}"] = {"diff_type": diff_type, "child": []}

        # 将任务状态与差异比对数据结构合并返回
        for instance in instance_statuses:
            for scope_id in instance["scope_ids"]:
                if scope_id in nodes:
                    nodes[scope_id]["child"].append(instance)
            # 清理scope_ids
            instance.pop("scope_ids", None)

        # 补充拓扑节点信息
        if (current_node_type, self.collect_config.target_object_type) == (
            TargetNodeType.INSTANCE,
            TargetObjectType.HOST,
        ):
            # 主机全部归属在主机节点下
            diff_mapping = defaultdict(lambda: {"child": [], "node_path": _("主机")})
            for node_id, node_info in nodes.items():
                diff_mapping[node_info["diff_type"]]["child"].extend(node_info["child"])
            for diff_type, diff_info in diff_mapping.items():
                diff_info.update({"label_name": diff_type, "is_label": bool(diff_type)})
        else:
            # 服务/集群模板归属在对应的服务/集群模板下
            topo_links = self._get_topo_links()
            diff_mapping = {}
            for node_id, node_info in nodes.items():
                node_path = "/".join(link.bk_inst_name for link in reversed(topo_links.get(node_id, [])))
                if not node_path:
                    node_path = f"{_('未知节点')}({node_id})"
                diff_mapping[node_info["diff_type"]] = {
                    "child": node_info["child"],
                    "node_path": node_path,
                    "label_name": node_info["diff_type"],
                    "is_label": bool(node_info["diff_type"]),
                }

        return list(diff_mapping.values())