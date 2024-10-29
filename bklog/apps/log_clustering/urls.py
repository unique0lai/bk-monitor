# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-LOG 蓝鲸日志平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-LOG 蓝鲸日志平台 is licensed under the MIT License.
License for BK-LOG 蓝鲸日志平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
We undertake not to change the open source license (MIT license) applicable to the current version of
the project delivered to anyone in the future.
"""
from django.conf.urls import include, url
from rest_framework import routers

from apps.log_clustering.views.clustering_config_views import ClusteringConfigViewSet
from apps.log_clustering.views.clustering_monitor_views import ClusteringMonitorViewSet
from apps.log_clustering.views.pattern_views import PatternViewSet
from apps.log_clustering.views.regex_template_views import RegexTemplateViewSet
from apps.log_clustering.views.report_views import ReportViewSet

router = routers.DefaultRouter(trailing_slash=True)
router.register(r"pattern", PatternViewSet, basename="pattern_set")
router.register(r"report", ReportViewSet, basename="report")
router.register(r"clustering_config", ClusteringConfigViewSet, basename="clustering_config")
router.register(r"clustering_monitor", ClusteringMonitorViewSet, basename="clustering_monitor")
router.register(r"regex_template", RegexTemplateViewSet, basename="regex_template")

urlpatterns = [
    url(r"^", include(router.urls)),
]
