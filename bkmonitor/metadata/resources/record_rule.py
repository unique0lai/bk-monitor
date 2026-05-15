"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.exceptions import ValidationError

from bkmonitor.utils.request import get_request_tenant_id
from bkmonitor.utils.serializers import TenantIdField
from core.drf_resource import Resource
from metadata.models.record_rule.constants import (
    RECORD_RULE_V4_DEFAULT_REFRESH_INTERVAL,
    RECORD_RULE_V4_INTERVAL_CHOICES,
    RecordRuleV4InputType,
)
from metadata.models.record_rule.v4 import RecordRuleV4


class RecordRuleV4BaseSerializer(serializers.Serializer):
    bk_tenant_id = TenantIdField(label="租户ID", required=False)


class RecordRuleV4ConfigSerializer(RecordRuleV4BaseSerializer):
    input_type = serializers.ChoiceField(
        label="输入类型", choices=[item.value for item in RecordRuleV4InputType], required=True
    )
    input_config = serializers.DictField(label="用户原始输入", required=True)
    metric_name = serializers.CharField(label="输出指标名", required=True)
    labels = serializers.ListField(label="附加标签", child=serializers.DictField(), required=False, default=list)
    interval = serializers.ChoiceField(label="计算周期", choices=RECORD_RULE_V4_INTERVAL_CHOICES, default="1min")
    auto_refresh = serializers.BooleanField(label="是否自动刷新", required=False, default=True)
    refresh_interval = serializers.IntegerField(
        label="刷新间隔(秒)", required=False, default=RECORD_RULE_V4_DEFAULT_REFRESH_INTERVAL
    )


class CreateRecordRuleV4Resource(Resource):
    """创建 V4 预计算规则"""

    class RequestSerializer(RecordRuleV4ConfigSerializer):
        space_type = serializers.CharField(label="空间类型", required=True)
        space_id = serializers.CharField(label="空间ID", required=True)
        record_name = serializers.CharField(label="预计算名称", required=True)

    def perform_request(self, validated_request_data):
        validated_request_data["bk_tenant_id"] = validated_request_data.get("bk_tenant_id") or get_request_tenant_id()
        rule = RecordRuleV4.create_rule(**validated_request_data)
        return rule.to_dict()


class ModifyRecordRuleV4Resource(Resource):
    """修改 V4 预计算规则"""

    class RequestSerializer(RecordRuleV4BaseSerializer):
        id = serializers.IntegerField(label="规则ID", required=True)
        input_type = serializers.ChoiceField(
            label="输入类型", choices=[item.value for item in RecordRuleV4InputType], required=False
        )
        input_config = serializers.DictField(label="用户原始输入", required=False)
        metric_name = serializers.CharField(label="输出指标名", required=False)
        labels = serializers.ListField(label="附加标签", child=serializers.DictField(), required=False)
        interval = serializers.ChoiceField(label="计算周期", choices=RECORD_RULE_V4_INTERVAL_CHOICES, required=False)
        auto_refresh = serializers.BooleanField(label="是否自动刷新", required=False)

    def perform_request(self, validated_request_data):
        rule = get_record_rule_v4(
            pk=validated_request_data.pop("id"),
            bk_tenant_id=validated_request_data.pop("bk_tenant_id", None),
        )
        rule.modify_rule(**validated_request_data)
        return rule.to_dict()


class DeleteRecordRuleV4Resource(Resource):
    """删除 V4 预计算规则"""

    class RequestSerializer(RecordRuleV4BaseSerializer):
        id = serializers.IntegerField(label="规则ID", required=True)

    def perform_request(self, validated_request_data):
        rule = get_record_rule_v4(
            pk=validated_request_data["id"],
            bk_tenant_id=validated_request_data.get("bk_tenant_id"),
        )
        rule.mark_deleted()
        return {"id": rule.pk, "status": rule.status}


class GetRecordRuleV4Resource(Resource):
    """查询 V4 预计算规则详情"""

    class RequestSerializer(RecordRuleV4BaseSerializer):
        id = serializers.IntegerField(label="规则ID", required=True)

    def perform_request(self, validated_request_data):
        rule = get_record_rule_v4(
            pk=validated_request_data["id"],
            bk_tenant_id=validated_request_data.get("bk_tenant_id"),
        )
        return rule.to_dict()


class ListRecordRuleV4Resource(Resource):
    """查询 V4 预计算规则列表"""

    class RequestSerializer(RecordRuleV4BaseSerializer):
        space_type = serializers.CharField(label="空间类型", required=False, allow_blank=True)
        space_id = serializers.CharField(label="空间ID", required=False, allow_blank=True)
        record_name = serializers.CharField(label="预计算名称", required=False, allow_blank=True)
        status = serializers.CharField(label="状态", required=False, allow_blank=True)
        auto_refresh = serializers.BooleanField(label="是否自动刷新", required=False, allow_null=True, default=None)
        has_change = serializers.BooleanField(label="是否存在待更新变更", required=False, allow_null=True, default=None)

    def perform_request(self, validated_request_data):
        qs = RecordRuleV4.objects.all().order_by("-pk")
        bk_tenant_id = validated_request_data.get("bk_tenant_id") or get_request_tenant_id(peaceful=True)
        if bk_tenant_id:
            qs = qs.filter(bk_tenant_id=bk_tenant_id)
        for field in ["space_type", "space_id", "record_name", "status"]:
            value = validated_request_data.get(field)
            if value:
                qs = qs.filter(**{field: value})
        if validated_request_data.get("auto_refresh") is not None:
            qs = qs.filter(auto_refresh=validated_request_data["auto_refresh"])
        if validated_request_data.get("has_change") is not None:
            qs = qs.filter(has_change=validated_request_data["has_change"])
        return [rule.to_dict() for rule in qs]


class RefreshRecordRuleV4Resource(Resource):
    """手动刷新 V4 预计算规则"""

    class RequestSerializer(RecordRuleV4BaseSerializer):
        id = serializers.IntegerField(label="规则ID", required=True)

    def perform_request(self, validated_request_data):
        rule = get_record_rule_v4(
            pk=validated_request_data["id"],
            bk_tenant_id=validated_request_data.get("bk_tenant_id"),
        )
        changed = rule.refresh_if_changed(auto_apply=True)
        data = rule.to_dict()
        data["changed"] = changed
        return data


def get_record_rule_v4(pk: int, bk_tenant_id: str | None = None) -> RecordRuleV4:
    query = {"pk": pk}
    tenant_id = bk_tenant_id or get_request_tenant_id(peaceful=True)
    if tenant_id:
        query["bk_tenant_id"] = tenant_id
    try:
        return RecordRuleV4.objects.get(**query)
    except RecordRuleV4.DoesNotExist:
        raise ValidationError(_("V4 预计算规则不存在: {}").format(pk))
