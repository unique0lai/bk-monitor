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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from metadata.models.record_rule.v4.models import RecordRuleV4


class RecordRuleV4OutputResources:
    """维护 V4 recording rule 输出侧 metadata。

    ResultTable / AccessVMRecord 是 group 级资源，应该在 RecordRuleV4 创建
    后立刻准备好；指标字段则依赖 resolved/flow 展开结果，由部署执行前按需补齐。
    """

    @classmethod
    def ensure_group_output(cls, rule: RecordRuleV4) -> None:
        """创建 group 输出 RT 及对应 VM 写入映射。"""

        cls.ensure_result_table(rule)
        cls.ensure_vm_record(rule)

    @staticmethod
    def ensure_result_table(rule: RecordRuleV4) -> None:
        """创建输出 ResultTable，供后续 Flow output 引用。"""

        from metadata import models as metadata_models

        biz_id = metadata_models.Space.objects.get_biz_id_by_space(rule.space_type, rule.space_id)
        metadata_models.ResultTable.objects.get_or_create(
            bk_tenant_id=rule.bk_tenant_id,
            table_id=rule.table_id,
            defaults={
                "table_name_zh": rule.table_id,
                "is_custom_table": True,
                "default_storage": metadata_models.ClusterInfo.TYPE_VM,
                "creator": "system",
                "bk_biz_id": biz_id,
            },
        )

    @staticmethod
    def ensure_vm_record(rule: RecordRuleV4) -> None:
        """创建输出 RT 到 VM RT 的映射。"""

        from metadata import models as metadata_models
        from metadata.models.vm import utils as vm_utils

        vm_cluster_info = vm_utils.get_vm_cluster_id_name(
            bk_tenant_id=rule.bk_tenant_id,
            space_type=rule.space_type,
            space_id=rule.space_id,
        )
        metadata_models.AccessVMRecord.objects.get_or_create(
            bk_tenant_id=rule.bk_tenant_id,
            result_table_id=rule.table_id,
            defaults={
                "bk_base_data_id": 0,
                "vm_result_table_id": rule.dst_vm_table_id,
                "vm_cluster_id": vm_cluster_info["cluster_id"],
            },
        )

    @staticmethod
    def ensure_metric_fields(rule: RecordRuleV4, metric_names: list[str]) -> None:
        """按部署展开结果补齐输出指标字段。"""

        from metadata import models as metadata_models

        for metric_name in metric_names:
            metadata_models.ResultTableField.objects.get_or_create(
                bk_tenant_id=rule.bk_tenant_id,
                table_id=rule.table_id,
                field_name=metric_name,
                defaults={
                    "field_type": metadata_models.ResultTableField.FIELD_TYPE_FLOAT,
                    "description": metric_name,
                    "tag": metadata_models.ResultTableField.FIELD_TAG_METRIC,
                    "is_config_by_user": True,
                },
            )
