"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2024 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""
import logging
from copy import copy

from django.conf import settings
from django.core.paginator import Paginator
from django.db import transaction
from django.db.models import Q
from django.utils.translation import ugettext as _

from bkm_space.api import SpaceApi
from bkmonitor.utils import shortuuid
from bkmonitor.utils.local import local
from bkmonitor.utils.request import get_request
from bkmonitor.utils.user import get_global_user
from bkmonitor.views import serializers
from constants.cmdb import TargetNodeType, TargetObjectType
from core.drf_resource import api, resource
from core.drf_resource.base import Resource
from core.errors.api import BKAPIError
from core.errors.collecting import (
    CollectConfigNeedUpgrade,
    CollectConfigNotExist,
    CollectConfigNotNeedUpgrade,
    CollectConfigParamsError,
    CollectConfigRollbackError,
    DeleteCollectConfigError,
    SubscriptionStatusError,
    ToggleConfigStatusError,
)
from core.errors.plugin import PluginIDNotExist
from monitor_web.collecting.constant import (
    COLLECT_TYPE_CHOICES,
    CollectStatus,
    OperationResult,
    OperationType,
    Status,
    TaskStatus,
)
from monitor_web.collecting.lock import CacheLock, lock
from monitor_web.collecting.utils import fetch_sub_statistics
from monitor_web.models import (
    CollectConfigMeta,
    CollectorPluginMeta,
    DeploymentConfigVersion,
)
from monitor_web.plugin.constant import PluginType
from monitor_web.plugin.manager import PluginManagerFactory
from monitor_web.strategies.loader.datalink_loader import (
    DatalinkDefaultAlarmStrategyLoader,
)
from monitor_web.tasks import append_metric_list_cache
from utils import business

logger = logging.getLogger(__name__)


class CollectConfigListResource(Resource):
    """
    获取采集配置列表信息
    """

    def __init__(self):
        super(CollectConfigListResource, self).__init__()
        self.realtime_data = {}  # 采集配置实时数据结果
        self.service_type_data = {}  # 服务分类数据
        self.plugin_release_version = {}  # 插件最新版本，用于检查采集配置是否需要升级
        self.bk_biz_id = None

    class RequestSerializer(serializers.Serializer):
        bk_biz_id = serializers.IntegerField(required=False, label="业务ID")
        refresh_status = serializers.BooleanField(required=False, label="是否刷新状态")
        search = serializers.DictField(required=False, label="搜索字段")
        order = serializers.CharField(required=False, label="排序字段")
        disable_service_type = serializers.BooleanField(default=True, label="不需要服务分类")
        page = serializers.IntegerField(required=False, default=1, label="页数")
        limit = serializers.IntegerField(required=False, default=10, label="大小")

    def get_realtime_data(self, config_data_list):
        """
        获取节点管理订阅实时状态
        :param config_data_list: 采集配置数据列表
        :return: self.realtime_data
        """

        subscription_id_config_map, statistics_data = fetch_sub_statistics(config_data_list)

        # 节点管理返回的状态数量
        for subscription_status in statistics_data:
            status_number = {}
            for status_result in subscription_status.get("status", []):
                status_number[status_result["status"]] = status_result["count"]

            error_count = status_number.get(CollectStatus.FAILED, 0)
            total_count = subscription_status.get("instances", 0)
            pending_count = status_number.get(CollectStatus.PENDING, 0)
            running_count = status_number.get(CollectStatus.RUNNING, 0)
            subscription_status_data = {
                "error_instance_count": error_count,
                "total_instance_count": total_count,
                "pending_instance_count": pending_count,
                "running_instance_count": running_count,
            }
            self.realtime_data.update({subscription_status["subscription_id"]: subscription_status_data})

            # 更新任务状态
            config = subscription_id_config_map[subscription_status["subscription_id"]]
            if not config:
                continue
            if error_count == 0:
                operation_result = OperationResult.SUCCESS
            elif error_count == total_count:
                operation_result = OperationResult.FAILED
            elif running_count + pending_count != 0:
                operation_result = OperationResult.DEPLOYING
            else:
                operation_result = OperationResult.WARNING

            # 更新缓存
            cache_data = {
                "error_instance_count": subscription_status_data.get("error_instance_count", 0),
                "total_instance_count": subscription_status_data.get("total_instance_count", 0),
            }
            if config.cache_data != cache_data or config.operation_result != operation_result:
                CollectConfigMeta.objects.filter(id=config.id).update(
                    cache_data=cache_data, operation_result=operation_result
                )

                # 更新内存数据
                config.cache_data = cache_data
                config.operation_result = operation_result

    def update_cache_data(self, config):
        # 更新采集配置的缓存数据（总数、异常数）
        subscription_id = config.deployment_config.subscription_id
        realtime_data = self.realtime_data.get(subscription_id)
        if not realtime_data:
            return

        cache_data = {
            "error_instance_count": realtime_data.get("error_instance_count", 0),
            "total_instance_count": realtime_data.get("total_instance_count", 0),
        }
        # 若缓存数据和实际数据不一致，则更新数据库
        if config.cache_data != cache_data:
            CollectConfigMeta.objects.filter(id=config.id).update(cache_data=cache_data)

        # 更新内存数据
        config.cache_data = cache_data

    @staticmethod
    def update_cache_data_item(conf, field, value):
        """
        更新缓存数据某字段
        :param conf: 采集配置
        :param field: 字段
        :param value: 值
        :return: conf
        """
        if not isinstance(conf.cache_data, dict):
            conf.cache_data = {}
        conf.cache_data[field] = value
        return conf

    def get_status(self, conf):
        # 判断采集配置是否处于自动下发中，返回采集配置状态和任务状态
        status_key = conf.deployment_config.subscription_id
        if self.realtime_data.get(status_key) and self.realtime_data.get(status_key).get("is_auto_deploying"):
            status = {
                "config_status": Status.AUTO_DEPLOYING,
                "task_status": TaskStatus.AUTO_DEPLOYING,
                "running_tasks": self.realtime_data.get(status_key).get("auto_running_tasks"),
            }
        else:
            status = {"config_status": conf.config_status, "task_status": conf.task_status, "running_tasks": []}

        conf = self.update_cache_data_item(conf, "status", conf.config_status)
        conf = self.update_cache_data_item(conf, "task_status", conf.task_status)
        conf.save(not_update_user=True, update_fields=["cache_data"])
        return status

    def _need_upgrade(self, conf):
        # 判断采集配置是否需要升级，使用config_version缓存，大幅减少查询数据库的次数
        # 如果采集配置处于已停用，或者主机/实例总数为零，则不需要进行升级
        if conf.task_status == TaskStatus.STOPPED or conf.get_cache_data("total_instance_count", 0) == 0:
            return False
        else:
            config_version = self.plugin_release_version.get(conf.plugin.plugin_id)
            if not config_version:
                config_version = conf.plugin.packaged_release_version.config_version
                self.plugin_release_version[conf.plugin.plugin_id] = config_version

            return conf.deployment_config.plugin_version.config_version < config_version

    def need_upgrade(self, conf):
        # 判断采集配置是否需要升级，使用config_version缓存，大幅减少查询数据库的次数
        # 如果采集配置处于已停用，或者主机/实例总数为零，则不需要进行升级
        is_need_upgrade = self._need_upgrade(conf)
        conf = self.update_cache_data_item(conf, "need_upgrade", is_need_upgrade)
        conf.save(not_update_user=True, update_fields=["cache_data"])
        return is_need_upgrade

    def perform_request(self, validated_request_data):
        try:
            bk_biz_id = validated_request_data.get("bk_biz_id")
            refresh_status = validated_request_data.get("refresh_status")
            search_dict = validated_request_data.get("search", {})
            order = validated_request_data.get("order")
            self.bk_biz_id = bk_biz_id

            collect_config_fields = [i.attname for i in list(CollectConfigMeta._meta.fields)]
            new_search = []
            for item, value in search_dict.items():
                if item in ["status", "task_status"]:
                    # config_status: 启用 STARTED、停用 STOPPED
                    # task_status: 异常 WARNING
                    new_search.append(Q(cache_data__contains=f'"{item}": "{value}"'))
                elif item == "need_upgrade":
                    # need_upgrade: 是否需要升级
                    new_search.append(Q(cache_data__contains=f'"{item}": {value}'))
                elif item == "fuzzy":
                    new_search.append(Q(id__icontains=value) | Q(name__icontains=value))
                elif item in collect_config_fields:
                    new_search.append(Q(**{item: value}))

            # 获取全量的采集配置数据（包含外键数据）filter(**search_dict)
            config_list = (
                CollectConfigMeta.objects.filter(*new_search)
                .select_related("plugin", "deployment_config__plugin_version")
                .order_by("-id")
            )

            all_space_list = SpaceApi.list_spaces()
            bk_biz_id_space_dict = {space.bk_biz_id: space for space in all_space_list}

            global_plugins = CollectorPluginMeta.objects.filter(bk_biz_id=0).values("plugin_type", "plugin_id")

            # bk_biz_id可以为空，为空则按用户拥有的业务查询
            if bk_biz_id:
                space = bk_biz_id_space_dict.get(bk_biz_id)
                data_sources = api.metadata.query_data_source_by_space_uid(
                    space_uid_list=[space.space_uid], is_platform_data_id=True
                )
                data_names = [ds["data_name"] for ds in data_sources]
                plugin_ids = []
                for plugin in global_plugins:
                    data_name = f"{plugin['plugin_type']}_{plugin['plugin_id']}".lower()
                    if data_name in data_names:
                        plugin_ids.append(plugin['plugin_id'])

                filter_condition = Q(plugin_id__in=plugin_ids) | Q(bk_biz_id=bk_biz_id)
            else:
                # 全业务场景 to be legacy
                plugin_ids = []
                user_biz_ids = resource.space.get_bk_biz_ids_by_user(get_request().user)
                space_uid_set = set()
                for biz_id in user_biz_ids:
                    space = bk_biz_id_space_dict.get(biz_id)
                    if space:
                        space_uid_set.add(space.space_uid)

                data_sources = api.metadata.query_data_source_by_space_uid(
                    space_uid_list=list(space_uid_set), is_platform_data_id=True
                )
                data_names = [ds["data_name"] for ds in data_sources]
                for plugin in global_plugins:
                    data_name = f"{plugin['plugin_type']}_{plugin['plugin_id']}".lower()
                    if data_name in data_names:
                        plugin_ids.append(plugin['plugin_id'])
                # 用户拥有业务下创建的插件以及业务下的采集
                filter_condition = Q(plugin_id__in=plugin_ids) | Q(bk_biz_id__in=user_biz_ids)

            config_list = config_list.filter(filter_condition)

            total = len(config_list)
            if total == 0:
                return {"type_list": [], "config_list": [], "total": 0}

            if validated_request_data["page"] != -1:
                paginator = Paginator(config_list, validated_request_data["limit"])
                config_data_list = list(paginator.page(validated_request_data["page"]))
            else:
                config_data_list = list(config_list)

            if refresh_status:
                try:
                    self.get_realtime_data(config_data_list)
                except Exception:
                    # 尝试实时获取，获取失败就用缓存数据
                    pass

            search_list = []
            for item in config_data_list:
                status = self.get_status(item)
                space = bk_biz_id_space_dict.get(item.bk_biz_id)
                search_list.append(
                    {
                        "id": item.id,
                        "name": item.name,
                        "bk_biz_id": item.bk_biz_id,
                        "space_name": f"{space.space_name}({space.type_name})" if space else "",
                        "collect_type": item.collect_type,
                        "status": status["config_status"],
                        "task_status": status["task_status"],
                        "target_object_type": item.target_object_type,
                        "target_node_type": item.deployment_config.target_node_type,
                        "plugin_id": item.plugin.plugin_id,
                        "target_nodes_count": len(item.deployment_config.target_nodes),
                        "need_upgrade": self.need_upgrade(item),
                        "config_version": item.deployment_config.plugin_version.config_version,
                        "info_version": item.deployment_config.plugin_version.info_version,
                        "error_instance_count": (
                            0
                            if status["task_status"] == TaskStatus.STOPPED
                            else item.get_cache_data("error_instance_count", 0)
                        ),
                        "total_instance_count": item.get_cache_data("total_instance_count", 0),
                        "running_tasks": status["running_tasks"],
                        "label_info": item.label_info,
                        "label": item.label,
                        "update_time": item.update_time,
                        "update_user": item.update_user,
                    }
                )

            # 排序
            if order:
                reverse = False
                if order.startswith("-"):
                    order = order[1:]
                    reverse = True

                try:
                    search_list.sort(key=lambda x: x[order], reverse=reverse)
                except KeyError:
                    pass

            # 获取插件类型
            type_list = [{"id": item[0], "name": item[1]} for item in COLLECT_TYPE_CHOICES]

            return {"type_list": type_list, "config_list": search_list, "total": total}
        except Exception:
            import traceback

            logger.error(traceback.format_exc())

        return {"type_list": [], "config_list": [], "total": 0}


class CollectConfigDetailResource(Resource):
    """
    获取采集配置详细信息
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")

    @staticmethod
    def password_convert(collect_config_meta):
        """
        将密码类型的参数转换为一个bool值，用于规避f12可以看到明文密码
        @param collect_config_meta:
        @return:
        """
        config_json = collect_config_meta.deployment_config.plugin_version.config.config_json
        params = collect_config_meta.deployment_config.params
        for item in config_json:
            if item["mode"] != "collector":
                item["mode"] = "plugin"
            value = params.get(item["mode"], {}).get(item.get("key", item["name"])) or item["default"]
            # 获取敏感信息时采用bool值表示用户是否设置密码
            if item["type"] in ["password", "encrypt"]:
                params[item["mode"]][item["name"]] = bool(value)

    def perform_request(self, validated_request_data):
        config_id = validated_request_data["id"]
        try:
            collect_config_meta = CollectConfigMeta.objects.select_related("deployment_config").get(id=config_id)
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": config_id})

        # 请求IP选择器接口，获取采集目标
        if (
            collect_config_meta.target_object_type == TargetObjectType.HOST
            and collect_config_meta.deployment_config.target_node_type == TargetNodeType.INSTANCE
        ):
            target_result = resource.commons.get_host_instance_by_ip(
                {
                    "bk_biz_id": collect_config_meta.bk_biz_id,
                    "bk_biz_ids": [collect_config_meta.bk_biz_id],
                    "ip_list": collect_config_meta.deployment_config.target_nodes,
                }
            )
        elif (
            collect_config_meta.target_object_type == TargetObjectType.HOST
            and collect_config_meta.deployment_config.target_node_type == TargetNodeType.TOPO
        ):
            node_list = []
            for item in collect_config_meta.deployment_config.target_nodes:
                item.update({"bk_biz_id": collect_config_meta.bk_biz_id})
                node_list.append(item)
            target_result = resource.commons.get_host_instance_by_node(
                {"bk_biz_id": collect_config_meta.bk_biz_id, "node_list": node_list}
            )
        elif collect_config_meta.target_object_type in [
            TargetObjectType.HOST,
            TargetObjectType.SERVICE,
        ] and collect_config_meta.deployment_config.target_node_type in [
            TargetNodeType.SERVICE_TEMPLATE,
            TargetNodeType.SET_TEMPLATE,
        ]:
            target_result = []
            templates = {
                template["bk_inst_id"]: template["bk_inst_name"]
                for template in resource.commons.get_template(
                    dict(
                        bk_biz_id=collect_config_meta.bk_biz_id,
                        bk_obj_id=collect_config_meta.deployment_config.target_node_type,
                        bk_inst_type=collect_config_meta.target_object_type,
                    )
                ).get("children", [])
            }
            for item in collect_config_meta.deployment_config.target_nodes:
                item.update({"bk_biz_id": collect_config_meta.bk_biz_id})
                item.update({"bk_inst_name": templates.get(item["bk_inst_id"])})
                target_result.append(item)
        else:
            node_list = []
            for item in collect_config_meta.deployment_config.target_nodes:
                item.update({"bk_biz_id": collect_config_meta.bk_biz_id})
                node_list.append(item)
            target_result = resource.commons.get_service_instance_by_node(
                {"bk_biz_id": collect_config_meta.bk_biz_id, "node_list": node_list}
            )
        config_version = collect_config_meta.deployment_config.plugin_version.config_version
        release_version = collect_config_meta.plugin.get_release_ver_by_config_ver(config_version)
        # 密码转为非明文
        self.password_convert(collect_config_meta)
        result = {
            "id": collect_config_meta.id,
            "deployment_id": collect_config_meta.deployment_config_id,
            "name": collect_config_meta.name,
            "bk_biz_id": collect_config_meta.bk_biz_id,
            "collect_type": collect_config_meta.collect_type,
            "label": collect_config_meta.label,
            "target_object_type": collect_config_meta.target_object_type,
            "target_node_type": collect_config_meta.deployment_config.target_node_type,
            "target_nodes": collect_config_meta.deployment_config.target_nodes,
            "params": collect_config_meta.deployment_config.params,
            "remote_collecting_host": collect_config_meta.deployment_config.remote_collecting_host,
            "plugin_info": release_version.get_plugin_version_detail(),
            "target": target_result,
            "subscription_id": collect_config_meta.deployment_config.subscription_id,
            "label_info": collect_config_meta.label_info,
            "create_time": collect_config_meta.create_time,
            "create_user": collect_config_meta.create_user,
            "update_time": collect_config_meta.update_time,
            "update_user": collect_config_meta.update_user,
        }
        return result


class RenameCollectConfigResource(Resource):
    """
    编辑采集配置的名称
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")
        name = serializers.CharField(required=True, label="名称")

    def perform_request(self, data):
        try:
            collect_config = CollectConfigMeta.objects.get(id=data["id"])
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": data["id"]})
        collect_config.name = data["name"]
        collect_config.save()
        return "success"


class ToggleCollectConfigStatusResource(Resource):
    """
    启停采集配置
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")
        action = serializers.ChoiceField(required=True, choices=["enable", "disable"], label="启停配置")

    @lock(CacheLock("collect_config"))
    def request_nodeman(self, collect_config, action):
        if action == "disable":
            collect_config.switch_subscription("disable")
        elif action == "enable" and settings.IS_SUBSCRIPTION_ENABLED:
            collect_config.switch_subscription("enable")
        task_id = collect_config.trigger_subscription(action="START" if action == "enable" else "STOP")
        return task_id

    def perform_request(self, validated_request_data):
        config_id = validated_request_data["id"]
        action = validated_request_data["action"]
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(id=config_id)
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": config_id})

        # 判断采集配置是否可以启用/停用
        if (
            action == "enable"
            and collect_config.config_status != Status.STOPPED
            or action == "disable"
            and collect_config.config_status != Status.STARTED
        ):
            raise ToggleConfigStatusError({"msg": _("采集配置未处于已启用/已停用状态")})

        # 请求节点管理，更新采集配置任务状态
        if collect_config.deployment_config.subscription_id:
            task_id = self.request_nodeman(collect_config, action)
            collect_config.deployment_config.task_ids = [task_id]
            collect_config.deployment_config.save()
            collect_config.last_operation = OperationType.START if action == "enable" else OperationType.STOP
            collect_config.operation_result = OperationResult.PREPARING
            collect_config.save()
        else:
            collect_config.last_operation = OperationType.START if action == "enable" else OperationType.STOP
            collect_config.operation_result = OperationResult.SUCCESS
            collect_config.save()
        return "success"


class DeleteCollectConfigResource(Resource):
    """
    删除采集配置
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")

    def perform_request(self, data):
        # 获取采集配置
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(id=data["id"])
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": data["id"]})

        # 判断采集配置是否停用
        if collect_config.task_status != TaskStatus.STOPPED and collect_config.deployment_config.subscription_id:
            raise DeleteCollectConfigError({"msg": _("采集配置未停用")})

        if collect_config.deployment_config.subscription_id:
            collect_config.delete_subscription()

        # 删除采集配置及部署配置
        DeploymentConfigVersion.objects.filter(config_meta_id=data["id"]).delete()
        collect_config.delete()

        # 内置链路健康策略处理
        # 如果用户还创建了其他的采集配置，则不会从告警组中移除
        username = get_global_user()
        bk_biz_id = collect_config.bk_biz_id
        configs_exist = CollectConfigMeta.objects.filter(bk_biz_id=bk_biz_id, create_user=username).exists()
        loader = DatalinkDefaultAlarmStrategyLoader(collect_config=collect_config, user_id=username)
        loader.delete(remove_user_from_group=not configs_exist)
        return None


class CloneCollectConfigResource(Resource):
    """
    克隆采集配置
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")

    def perform_request(self, data):
        # 获取采集配置
        data = resource.collecting.collect_config_detail(data)
        if (
            data["collect_type"] == CollectConfigMeta.CollectType.LOG
            or data["collect_type"] == CollectConfigMeta.CollectType.SNMP_TRAP
        ):
            #  判断重名
            new_name = name = data["name"] + "_copy"
            i = 1
            while CollectConfigMeta.objects.filter(name=new_name):
                new_name = f"{name}({i})"  # noqa
                i += 1
            data["name"] = new_name
            data.pop("id")
            # 日志类的插件id在创建时是default_log
            data["plugin_id"] = "default_log"
            # 克隆任务不克隆目标节点
            data["target_nodes"] = []

            result = resource.collecting.save_collect_config(data)
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(id=result["id"])
            try:
                update_config_operation_result(collect_config)
            except SubscriptionStatusError as e:
                logger.error(str(e))
            return None
        else:
            try:
                collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(id=data["id"])
            except CollectConfigMeta.DoesNotExist:
                raise CollectConfigNotExist({"msg": data["id"]})

            with transaction.atomic():
                # 克隆部署配置
                deployment_config = copy(collect_config.deployment_config)
                deployment_config.id = None
                # 克隆任务不克隆目标节点
                deployment_config.target_nodes = []
                deployment_config.subscription_id = 0
                deployment_config.save()
                # 克隆采集配置
                collect_config.id = None
                collect_config.deployment_config = deployment_config

                #  判断重名
                new_name = name = collect_config.name + "_copy"
                i = 1
                while CollectConfigMeta.objects.filter(name=new_name):
                    new_name = f"{name}({i})"
                    i += 1
                collect_config.name = new_name

                # 清除目标节点统计
                collect_config.cache_data = {}
                # 设置任务状态为“正常”
                collect_config.last_operation = OperationType.CREATE
                collect_config.operation_result = OperationResult.SUCCESS
                collect_config.save()
            return None


class RetryTargetNodesResource(Resource):
    """
    重试部分实例或主机
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")
        instance_id = serializers.CharField(required=True, label="需要重试的实例id")

    def perform_request(self, validated_request_data):
        config_id = validated_request_data["id"]
        instance_id = validated_request_data["instance_id"]
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(id=config_id)
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": config_id})

        # 主动触发节点管理订阅，更新采集配置信息
        if collect_config.deployment_config.subscription_id:
            task_id = collect_config.retry_subscription(instance_id_list=[instance_id])
            collect_config.deployment_config.task_ids.append(task_id)
            collect_config.deployment_config.save()
            collect_config.operation_result = OperationResult.PREPARING
            collect_config.save()
        return "success"


class RevokeTargetNodesResource(Resource):
    """
    终止部分部署中的实例
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")
        instance_ids = serializers.ListField(required=True, label="需要终止的实例ID")

    def perform_request(self, validated_request_data):
        config_id = validated_request_data["id"]
        instance_ids = validated_request_data["instance_ids"]
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(id=config_id)
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": config_id})

        # 主动触发节点管理终止任务
        if collect_config.deployment_config.subscription_id:
            api.node_man.revoke_subscription(
                subscription_id=collect_config.deployment_config.subscription_id, instance_id_list=instance_ids
            )
            update_config_operation_result(collect_config, not_update_user=False)
        return "success"


class BatchRevokeTargetNodesResource(Resource):
    """
    批量终止采集配置的部署中的实例
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")

    def perform_request(self, validated_request_data):
        config_id = validated_request_data["id"]
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(id=config_id)
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": config_id})

        # 主动触发节点管理终止任务
        # 不带 instance_id_list 即为批量终止
        if collect_config.deployment_config.subscription_id:
            api.node_man.revoke_subscription(subscription_id=collect_config.deployment_config.subscription_id)
            update_config_operation_result(collect_config, not_update_user=False)
        return "success"


class GetCollectLogDetailResource(Resource):
    """
    获取采集下发单台主机/实例的详细日志信息
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")
        instance_id = serializers.CharField(required=True, label="主机/实例id")
        task_id = serializers.IntegerField(required=True, label="任务id")

    def perform_request(self, validated_request_data):
        # todo 目前的日志是由后端拼接成文本，然后给前端显示的。和产品讨论后，后面会采取结构化的数据展示，等待最新的设计稿
        config_id = validated_request_data["id"]
        instance_id = validated_request_data["instance_id"]
        task_id = validated_request_data["task_id"]
        try:
            config = CollectConfigMeta.objects.select_related("deployment_config").get(id=config_id)
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": config_id})

        params = {
            "subscription_id": config.deployment_config.subscription_id,
            "instance_id": instance_id,
            "task_id": task_id,
        }
        result = api.node_man.task_result_detail(**params)
        if result:
            log = []
            for step in result.get("steps", []):
                log.append("{}{}{}\n".format("=" * 20, step["node_name"], "=" * 20))
                for sub_step in step["target_hosts"][0].get("sub_steps", []):
                    log.extend(["{}{}{}".format("-" * 20, sub_step["node_name"], "-" * 20), sub_step["log"]])
                    # 如果ex_data里面有值，则在日志里加上它
                    if sub_step["ex_data"]:
                        log.append(sub_step["ex_data"])
                    if sub_step["status"] != CollectStatus.SUCCESS:
                        return {"log_detail": "\n".join(log), "nodeman_result": result}
            return {"log_detail": "\n".join(log), "nodeman_result": result}
        else:
            return {"log_detail": _("未找到节点管理的日志"), "nodeman_result": result}


class BatchRetryConfigResource(Resource):
    """
    新建页面
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置ID")

    def __init__(self):
        super(BatchRetryConfigResource, self).__init__()
        self.config = None

    def get_node(self, instance):
        if self.config.target_object_type == TargetObjectType.HOST:
            return {
                "ip": instance["instance_info"]["host"]["bk_host_innerip"],
                "bk_cloud_id": int(instance["instance_info"]["host"]["bk_cloud_id"]),
                "bk_supplier_id": instance["instance_info"]["host"]["bk_supplier_account"],
            }
        else:
            return {"id": instance["instance_info"]["service"]["id"]}

    def get_failed_instances(self):
        params = {
            "subscription_id": self.config.deployment_config.subscription_id,
            "task_id_list": self.config.deployment_config.task_ids,
        }
        result = api.node_man.batch_task_result(**params)

        # 所有不正确的实例
        failed_instances_ids = [
            item["instance_id"] for item in result if item["status"] in [CollectStatus.FAILED, CollectStatus.PENDING]
        ]
        return failed_instances_ids, len(failed_instances_ids) == len(result)

    def perform_request(self, validated_request_data):
        config_id = validated_request_data["id"]
        try:
            self.config = CollectConfigMeta.objects.select_related("deployment_config").get(id=config_id)
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": config_id})

        failed_instances_ids, is_all_failed = self.get_failed_instances()
        params = {}
        if not is_all_failed:
            params["instance_id_list"] = failed_instances_ids
        task_id = self.config.retry_subscription(**params)
        self.config.deployment_config.task_ids.append(task_id)
        self.config.deployment_config.save()
        self.config.operation_result = OperationResult.PREPARING
        self.config.save()
        return "success"


class SaveCollectConfigResource(Resource):
    """
    新增或编辑采集配置
    """

    class RequestSerializer(serializers.Serializer):
        class RemoteCollectingSlz(serializers.Serializer):
            ip = serializers.CharField(required=False)
            bk_cloud_id = serializers.IntegerField(required=False)
            bk_host_id = serializers.IntegerField(required=False)
            bk_supplier_id = serializers.IntegerField(required=False)
            is_collecting_only = serializers.BooleanField(required=True)

            def validate(self, attrs):
                if "bk_host_id" not in attrs and not ("ip" in attrs and "bk_cloud_id" in attrs):
                    raise serializers.ValidationError(_("主机id和ip/bk_cloud_id不能同时为空"))
                return attrs

        class MetricRelabelConfigSerializer(serializers.Serializer):
            """指标重新标记配置对应的模板变量序列化器。

            对应模板
            {% if metric_relabel_configs %}
                metric_relabel_configs:
            {% for config in metric_relabel_configs %}
                 - source_labels: [{{ config.source_labels | join("', '") }}]
                    {% if config.regex %}regex: '{{ config.regex }}'{% endif %}
                    action: {{ config.action }}
                    {% if config.target_label %}target_label: '{{ config.target_label }}'{% endif %}
                    {% if config.replacement %}replacement: '{{ config.replacement }}'{% endif %}
            {% endfor %}
            {% endif %}
            """

            source_labels = serializers.ListField(child=serializers.CharField(), label="源标签列表")
            regex = serializers.CharField(label="正则表达式")
            action = serializers.CharField(required=False, label="操作类型")
            target_label = serializers.CharField(required=False, label="目标标签")
            replacement = serializers.CharField(required=False, label="替换内容")

        id = serializers.IntegerField(required=False, label="采集配置ID")
        name = serializers.CharField(required=True, label="采集配置名称")
        bk_biz_id = serializers.IntegerField(required=True, label="业务ID")
        collect_type = serializers.ChoiceField(
            required=True, label="采集方式", choices=CollectConfigMeta.COLLECT_TYPE_CHOICES
        )
        target_object_type = serializers.ChoiceField(
            required=True, label="采集对象类型", choices=CollectConfigMeta.TARGET_OBJECT_TYPE_CHOICES
        )
        target_node_type = serializers.ChoiceField(
            required=True, label="采集目标类型", choices=DeploymentConfigVersion.TARGET_NODE_TYPE_CHOICES
        )
        plugin_id = serializers.CharField(required=True, label="插件ID")
        target_nodes = serializers.ListField(required=True, label="节点列表")
        remote_collecting_host = RemoteCollectingSlz(required=False, allow_null=True, default=None, label="远程采集配置")
        params = serializers.DictField(required=True, label="采集配置参数")
        label = serializers.CharField(required=True, label="二级标签")
        operation = serializers.ChoiceField(default="EDIT", choices=["EDIT", "ADD_DEL"], label="操作类型")
        # 供第三方接口调用
        metric_relabel_configs = MetricRelabelConfigSerializer(many=True, default=list, label="指标重新标记配置")

        def validate(self, attrs):
            # 校验采集对象类型和采集目标类型搭配是否正确，且不同类型的节点列表字段正确
            # 校验业务拓扑和服务拓扑
            if (attrs["target_object_type"], attrs["target_node_type"]) in [
                (TargetObjectType.HOST, TargetNodeType.TOPO),
                (TargetObjectType.SERVICE, TargetNodeType.TOPO),
            ]:
                for node in attrs["target_nodes"]:
                    if not ("bk_inst_id" in node and "bk_obj_id" in node):
                        raise serializers.ValidationError("target_nodes needs bk_inst_id and bk_obj_id")
            # 校验主机实例
            elif (attrs["target_object_type"], attrs["target_node_type"]) == (
                TargetObjectType.HOST,
                TargetNodeType.INSTANCE,
            ):
                for node in attrs["target_nodes"]:
                    if not ("ip" in node and "bk_cloud_id" in node) and "bk_host_id" not in node:
                        raise serializers.ValidationError("target_nodes needs ip, bk_cloud_id or bk_host_id")
            # 校验服务模板、集群模板
            elif (attrs["target_object_type"], attrs["target_node_type"]) in [
                (TargetObjectType.HOST, TargetNodeType.SERVICE_TEMPLATE),
                (TargetObjectType.HOST, TargetNodeType.SET_TEMPLATE),
                (TargetObjectType.SERVICE, TargetNodeType.SET_TEMPLATE),
                (TargetObjectType.SERVICE, TargetNodeType.SERVICE_TEMPLATE),
            ]:
                for node in attrs["target_nodes"]:
                    if not ("bk_inst_id" in node and "bk_obj_id" in node):
                        raise serializers.ValidationError("target_nodes needs bk_inst_id, bk_obj_id")
            else:
                raise serializers.ValidationError(
                    "{} {} is not supported".format(attrs["target_object_type"], attrs["target_node_type"])
                )

            # 日志关键字规则名称去重
            if attrs["collect_type"] == CollectConfigMeta.CollectType.LOG:
                rules = attrs["params"]["log"]["rules"]

                name_set = set()
                for rule in rules:
                    rule_name = rule["name"]
                    if rule_name in name_set:
                        raise CollectConfigParamsError(msg="Duplicate keyword rule name({})".format(rule_name))
                    name_set.add(rule_name)

            return attrs

    @lock(CacheLock("collect_config"))
    def request_nodeman(self, collect_config, deployment_config):
        return collect_config.switch_config_version(deployment_config)

    def perform_request(self, data):
        try:
            collector_plugin = self.get_collector_plugin(data)
        except CollectorPluginMeta.DoesNotExist:
            raise PluginIDNotExist

        data["params"]["target_node_type"] = data["target_node_type"]
        data["params"]["target_object_type"] = data["target_object_type"]
        # 创建部署配置版本的参数
        if data["target_node_type"] in [
            TargetNodeType.SERVICE_TEMPLATE,
            TargetNodeType.SET_TEMPLATE,
            TargetNodeType.TOPO,
        ]:
            # 动态拓扑、集群模板、服务模板
            # todo 补齐对应跨业务下发权限控制
            target_nodes = data["target_nodes"]
        else:
            # 静态
            target_nodes = []
            for node in data["target_nodes"]:
                if "bk_host_id" in node:
                    target_nodes.append({"bk_host_id": node["bk_host_id"]})
                else:
                    target_nodes.append({"ip": node["ip"], "bk_cloud_id": node["bk_cloud_id"]})

        # 将重新标记配置参数嵌入到部署配置参数中
        data["params"]["collector"]["metric_relabel_configs"] = data.pop("metric_relabel_configs")

        deployment_config_params = {
            "plugin_version": collector_plugin.packaged_release_version,
            "target_node_type": data["target_node_type"],
            "params": data["params"],
            "target_nodes": target_nodes,
            "remote_collecting_host": data.get("remote_collecting_host"),
        }
        save_result = {}

        if data.get("id"):
            try:
                config_meta = CollectConfigMeta.objects.get(id=data["id"])
            except CollectConfigMeta.DoesNotExist:
                raise CollectConfigNotExist({"msg": data["id"]})

            self.update_password_inplace(data, config_meta)
            # 编辑
            collect_config = self.update_collector(data, deployment_config_params, save_result)
        else:
            try:
                # 新建
                collect_config = self.create_collector(data, deployment_config_params, collector_plugin)
            except Exception as err:
                logger.error(err)
                self.roll_back_result_table(collector_plugin)
                raise err

        # 异步更新主机总数的缓存
        resource.collecting.update_config_instance_count.delay(id=collect_config.id)

        save_result.update(id=collect_config.pk, deployment_id=collect_config.deployment_config_id)

        # 添加完成采集配置，主动更新指标缓存表
        self.update_metric_cache(collector_plugin)

        # 采集配置完成
        DatalinkDefaultAlarmStrategyLoader(collect_config=collect_config, user_id=get_global_user()).run()

        return save_result

    @staticmethod
    def update_password_inplace(data: dict, config_meta: "CollectConfigMeta") -> None:
        """将密码参数的值替换为实际值。"""
        config_params = config_meta.plugin.current_version.config.config_json
        deployment_params = config_meta.deployment_config.params

        for param in config_params:
            if param["type"] not in ["password", "encrypt"]:
                continue

            param_name = param["name"]
            param_mode = "plugin" if param["mode"] != "collector" else "collector"
            received_password = data["params"][param_mode].get(param_name)

            # mode 为 "plugin" 时，如果密码不改变，不会传入，获取到 None
            # mode 为 "collector" 时，如果密码不改变，传入值为 bool 类型（由详情接口返回的）
            # 这两种情况要替换为实际值（默认值兜底）
            if isinstance(received_password, (type(None), bool)):
                default_password = param["default"]
                actual_password = deployment_params[param_mode].get(param_name, default_password)
                data["params"][param_mode][param_name] = actual_password

    def update_collector(self, data, deployment_config_params, save_result):
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(pk=data["id"])
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": data["id"]})

        if collect_config.need_upgrade:
            raise CollectConfigNeedUpgrade({"msg": collect_config.name})

        # 请求节点管理，主动触发订阅，切换部署配置
        result = self.request_nodeman(collect_config, DeploymentConfigVersion(**deployment_config_params))

        # 更新采集配置信息
        can_rollback = self.update_collect_config(data, result)

        save_result.update({"diff_node": result["diff_result"]["nodes"], "can_rollback": can_rollback})
        return collect_config

    @staticmethod
    def update_collect_config(data, result):
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(pk=data["id"])
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": data["id"]})

        collect_config.name = data["name"]
        collect_config.label = data["label"]
        can_rollback = False
        if result["task_id"]:
            can_rollback = True
            collect_config.last_operation = data["operation"]
            collect_config.operation_result = OperationResult.PREPARING
            collect_config.deployment_config.task_ids = [result["task_id"]]
            collect_config.deployment_config.save()
        collect_config.save()
        return can_rollback

    @staticmethod
    def create_collector(data, deployment_config_params, collector_plugin):
        deployment_config_params["config_meta_id"] = 0

        with transaction.atomic():
            deployment_config = DeploymentConfigVersion.objects.create(**deployment_config_params)
            collect_config = CollectConfigMeta(
                bk_biz_id=data["bk_biz_id"],
                name=data["name"],
                last_operation=OperationType.CREATE,
                operation_result=OperationResult.PREPARING,
                collect_type=data["collect_type"],
                plugin=collector_plugin,
                target_object_type=data["target_object_type"],
                deployment_config=deployment_config,
                label=data["label"],
            )
            collect_config.deployment_config_id = deployment_config.id
            collect_config.save()
            result = collect_config.create_subscription()

        # 更新采集配置信息
        collect_config.deployment_config.subscription_id = result["subscription_id"]
        deployment_config.config_meta_id = collect_config.id
        deployment_config.task_ids = [result["task_id"]]
        deployment_config.save()
        # 新创建订阅是否开启巡检
        if settings.IS_SUBSCRIPTION_ENABLED:
            collect_config.switch_subscription("enable")
        else:
            collect_config.switch_subscription("disable")
        return collect_config

    @staticmethod
    def get_collector_plugin(data):
        plugin_id = data["plugin_id"]

        # 虚拟日志采集器
        if data["collect_type"] == CollectConfigMeta.CollectType.LOG:
            label = data["label"]
            bk_biz_id = data["bk_biz_id"]
            rules = data["params"]["log"]["rules"]
            if "id" not in data:
                plugin_id = "log_" + str(shortuuid.uuid())
                plugin_manager = PluginManagerFactory.get_manager(plugin=plugin_id, plugin_type=PluginType.LOG)
                params = plugin_manager.get_params(plugin_id, bk_biz_id, label, rules=rules)
                resource.plugin.create_plugin(params)
            else:
                plugin_manager = PluginManagerFactory.get_manager(plugin=plugin_id, plugin_type=PluginType.LOG)
                params = plugin_manager.get_params(plugin_id, bk_biz_id, label, rules=rules)
                plugin_manager.update_version(params)

        # 虚拟进程采集器
        if data["collect_type"] == CollectConfigMeta.CollectType.PROCESS:
            plugin_manager = PluginManagerFactory.get_manager("bkprocessbeat", plugin_type=PluginType.PROCESS)
            # 全局唯一
            plugin_manager.touch()
            plugin_id = plugin_manager.plugin.plugin_id

        if data["collect_type"] == CollectConfigMeta.CollectType.SNMP_TRAP:
            plugin_id = resource.collecting.get_trap_collector_plugin(data)
        collector_plugin = CollectorPluginMeta.objects.get(plugin_id=plugin_id)
        return collector_plugin

    @staticmethod
    def roll_back_result_table(collector_plugin):
        plugin_type = collector_plugin.plugin_type
        if plugin_type in collector_plugin.VIRTUAL_PLUGIN_TYPE:
            plugin_manager = PluginManagerFactory.get_manager(collector_plugin, plugin_type)
            plugin_manager.delete_result_table(collector_plugin.release_version)

    @staticmethod
    def update_metric_cache(collector_plugin):
        plugin_type = collector_plugin.plugin_type
        if plugin_type not in collector_plugin.VIRTUAL_PLUGIN_TYPE:
            version = collector_plugin.current_version
            metric_json = version.info.metric_json
            result_table_id_list = [
                "{}_{}.{}".format(
                    collector_plugin.plugin_type.lower(), collector_plugin.plugin_id, metric_msg["table_name"]
                )
                for metric_msg in metric_json
            ]
            append_metric_list_cache.delay(result_table_id_list)


class UpgradeCollectPluginResource(Resource):
    """
    采集配置插件升级
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置id")
        params = serializers.DictField(required=True, label="采集配置参数")
        realtime = serializers.BooleanField(required=False, default=False, label=_("是否实时刷新缓存"))

    @lock(CacheLock("collect_config"))
    def request_nodeman(self, collect_config, deployment_config):
        # 创建并切换到该部署配置
        return collect_config.switch_config_version(deployment_config)

    def perform_request(self, data):
        # 判断是否需要实时刷新缓存
        if data["realtime"]:
            # 调用 collect_config_list 接口刷新采集配置的缓存，避免外部调接口可能会无法更新插件
            resource.collecting.collect_config_list(page=-1, refresh_status=True, search={"id": data["id"]})

        try:
            collect_config = CollectConfigMeta.objects.select_related("plugin", "deployment_config").get(pk=data["id"])
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": data["id"]})

        # 判断采集配置是否需要升级
        if not collect_config.need_upgrade:
            raise CollectConfigNotNeedUpgrade({"msg": data["id"]})

        # 获取部署配置信息，请求节点管理
        data["params"]["collector"]["period"] = collect_config.deployment_config.params["collector"]["period"]
        deployment_config_params = {
            "plugin_version": collect_config.plugin.packaged_release_version,
            "target_node_type": collect_config.deployment_config.target_node_type,
            "params": data["params"],
            "target_nodes": collect_config.deployment_config.target_nodes,
            "remote_collecting_host": collect_config.deployment_config.remote_collecting_host,
        }
        result = self.request_nodeman(collect_config, DeploymentConfigVersion(**deployment_config_params))

        # 更新采集配置信息
        collect_config.deployment_config.task_ids = [result["task_id"]]
        collect_config.deployment_config.save()
        collect_config.last_operation = OperationType.UPGRADE
        collect_config.operation_result = OperationResult.PREPARING
        collect_config.save()

        # 升级采集配置，主动更新指标缓存表
        version = collect_config.plugin.current_version
        metric_json = version.info.metric_json
        result_table_id_list = [
            "{}_{}.{}".format(
                collect_config.plugin.plugin_type.lower(), collect_config.plugin.plugin_id, metric_msg["table_name"]
            )
            for metric_msg in metric_json
        ]
        append_metric_list_cache.delay(result_table_id_list)

        return {
            "id": collect_config.pk,
        }


class RollbackDeploymentConfigResource(Resource):
    """
    采集配置回滚
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置id")

    @lock(CacheLock("collect_config"))
    def request_nodeman(self, collect_config):
        return collect_config.rollback()

    def perform_request(self, data):
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(pk=data["id"])
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": data["id"]})

        # 判断是否采集配置是否允许回滚
        if not collect_config.allow_rollback:
            raise CollectConfigRollbackError({"msg": _("当前操作不支持回滚，或采集配置正处于执行中")})

        # 新克隆出的任务不需要进行节点管理调用
        if not collect_config.deployment_config.subscription_id:
            collect_config.last_operation = OperationType.ROLLBACK
            collect_config.operation_result = OperationResult.SUCCESS
            collect_config.save()
            return {"diff_node": []}

        # 请求节点管理，触发订阅，切换配置
        result = self.request_nodeman(collect_config)

        # 更新采集配置状态
        collect_config.deployment_config.task_ids = [result["task_id"]]
        collect_config.deployment_config.save()
        collect_config.last_operation = OperationType.ROLLBACK
        collect_config.operation_result = OperationResult.PREPARING
        collect_config.save()

        return {"diff_node": result["diff_result"]["nodes"]}


class GetMetricsResource(Resource):
    """
    获取对应插件版本的指标参数
    """

    class RequestSerializer(serializers.Serializer):
        id = serializers.IntegerField(required=True, label="采集配置id")

    def perform_request(self, validated_request_data):
        try:
            collect_config = CollectConfigMeta.objects.select_related("deployment_config").get(
                id=validated_request_data["id"]
            )
        except CollectConfigMeta.DoesNotExist:
            raise CollectConfigNotExist({"msg": validated_request_data["id"]})
        return collect_config.deployment_config.metrics


def update_config_operation_result(config, not_update_user=True):
    """
    更新采集配置的任务执行结果
    """
    local.username = business.maintainer(str(config.bk_biz_id))
    # 请求节点管理的任务结果接口，获取采集下发状态
    try:
        status_result = api.node_man.batch_task_result(subscription_id=config.deployment_config.subscription_id)
    except BKAPIError as e:
        message = _("采集配置 CollectConfigMeta: {} 查询订阅{}结果出错: {}").format(
            config.id, config.deployment_config.subscription_id, e
        )
        raise SubscriptionStatusError({"msg": message})
    except IndexError:
        message = _("采集配置 CollectConfigMeta: {} 对应订阅{}不存在").format(config.id, config.deployment_config.subscription_id)
        raise SubscriptionStatusError({"msg": message})

    error_count = 0
    total_count = len(status_result)
    instances_status = ""
    for item in status_result:
        instances_status += "{}({});".format(item["instance_id"], item["status"])
        if item["status"] in [CollectStatus.RUNNING, CollectStatus.PENDING]:
            logger.info("running instance is found in config {}".format(config.id))
            break
        if item["status"] == CollectStatus.FAILED:
            error_count += 1
    else:
        logger.info("采集配置id:{}, 实例状态:{}".format(config.id, instances_status))
        if error_count == 0:
            config.operation_result = OperationResult.SUCCESS
        elif error_count == total_count:
            config.operation_result = OperationResult.FAILED
        else:
            config.operation_result = OperationResult.WARNING
        config.save(not_update_user=not_update_user)


class CollectConfigInfoResource(Resource):
    """
    提供给kernel api使用，查询collect_config_meta表的信息
    """

    def perform_request(self, data):
        return list(CollectConfigMeta.objects.all().values())


class BatchRetryResource(BatchRetryConfigResource):
    """详情页"""

    pass