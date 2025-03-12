# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import pytest
from rest_framework.exceptions import ValidationError

from metadata import models
from metadata.resources.vm import NotifyDataLinkVmChange, QueryVmRtBySpace

pytestmark = pytest.mark.django_db


@pytest.fixture
def create_or_delete_records():
    models.ClusterInfo.objects.create(
        domain_name='test1.vm.db',
        cluster_name='test1',
        cluster_id=12345,
        cluster_type=models.ClusterInfo.TYPE_VM,
        port=1111,
        is_default_cluster=False,
    )
    models.ClusterInfo.objects.create(
        domain_name='test2.vm.db',
        cluster_name='test2',
        cluster_type=models.ClusterInfo.TYPE_VM,
        cluster_id=12346,
        port=1111,
        is_default_cluster=False,
    )
    models.AccessVMRecord.objects.create(
        vm_result_table_id='1001_test_vm', vm_cluster_id=11111111, bk_base_data_id=11111123
    )
    yield
    models.AccessVMRecord.objects.all().delete()
    models.ClusterInfo.objects.all().delete()


@pytest.mark.django_db(databases=['default', 'monitor_api'])
def test_notify_data_link_vm_change(create_or_delete_records):
    NotifyDataLinkVmChange().request(cluster_name='test1', vmrt='1001_test_vm')
    record = models.AccessVMRecord.objects.get(vm_result_table_id='1001_test_vm')
    assert record.vm_cluster_id == 12345

    with pytest.raises(ValidationError):
        NotifyDataLinkVmChange().request(cluster_name='test1', vmrt='1002_test_vm')


@pytest.mark.django_db(databases=['default', 'monitor_api'])
def test_query_vm_rt_without_plugin():
    params = {"space_type": "bkcc", "space_id": "0"}
    with pytest.raises(ValidationError):
        resp = QueryVmRtBySpace().request(params)
        # 如果 request 没有抛出 ValidationError，assert 失败
        assert resp
