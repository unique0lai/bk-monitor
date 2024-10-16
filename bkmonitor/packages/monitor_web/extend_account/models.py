"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""
from typing import Optional

from django.db import models
from monitor_web.extend_account.constants import VisitSource


class UserAccessRecordManager(models.Manager):
    """用户访问信息管理器"""

    @staticmethod
    def _get_visit_source(request) -> VisitSource:
        """通过 path 来获取访问来源"""
        if request.path.startswith("/weixin"):
            return VisitSource.MOBILE
        else:
            return VisitSource.PC

    @staticmethod
    def _get_bk_biz_id(request) -> Optional[str]:
        """通过 cookie 获取当前用户访问时的 biz 内容"""
        return request.COOKIES.get("bk_biz_id")

    def update_or_create_by_request(self, request) -> "UserAccessRecord":
        """通过 request 尝试创建"""
        extra_info, _ = self.update_or_create(
            username=request.user.username,
            source=self._get_visit_source(request).value,
            bk_biz_id=self._get_bk_biz_id(request),
        )
        return extra_info


class UserAccessRecord(models.Model):
    """用户访问信息"""

    username = models.CharField(verbose_name="用户名", max_length=64)
    source = models.CharField(verbose_name="最近一次访问来源", max_length=64)
    # 当前为了保证自动化 migrate，对 bk_biz_id 使用了 default 值，当新数据完全替代旧数据后，可统一清理
    bk_biz_id = models.CharField(verbose_name="业务ID", max_length=32, default="-1")
    updated_at = models.DateTimeField(verbose_name="修改时间", auto_now=True)
    created_at = models.DateTimeField(verbose_name="创建时间", auto_now_add=True)

    objects = UserAccessRecordManager()

    class Meta:
        unique_together = ("username", "source", "bk_biz_id")
