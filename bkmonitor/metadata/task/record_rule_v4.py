"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import logging

from metadata.models.record_rule.constants import RecordRuleV4Status
from metadata.models.record_rule.v4 import RecordRuleV4

logger = logging.getLogger("metadata")


def refresh_record_rule_v4():
    """定期检查并刷新 V4 预计算任务"""
    rules = RecordRuleV4.objects.filter(status=RecordRuleV4Status.RUNNING.value)
    logger.info("refresh_record_rule_v4: start refresh, count->[%s]", rules.count())
    for rule in rules.iterator():
        if not rule.is_refresh_due():
            continue
        try:
            changed = rule.refresh_if_changed(auto_apply=rule.auto_refresh)
            logger.info("refresh_record_rule_v4: rule_id->[%s], changed->[%s]", rule.pk, changed)
        except Exception as err:  # pylint: disable=broad-except
            logger.exception("refresh_record_rule_v4: refresh failed, rule_id->[%s], error->[%s]", rule.pk, err)
