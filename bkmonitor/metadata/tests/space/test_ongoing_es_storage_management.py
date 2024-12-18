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
import json
from datetime import timedelta

import pytest
from django.utils import timezone

from metadata import models
from metadata.models.space.space_table_id_redis import SpaceTableIDRedis
from metadata.tests.common_utils import consul_client

base_time = timezone.datetime(2020, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def create_or_delete_records(mocker):
    models.ESStorage.objects.create(table_id='1001_bklog.stdout', storage_cluster_id=11)
    models.ClusterInfo.objects.create(
        cluster_id=11,
        cluster_name="test_es_1",
        cluster_type=models.ClusterInfo.TYPE_ES,
        domain_name="es_test.1",
        port=9090,
        description="",
        is_default_cluster=True,
        version="5.x",
    )
    models.ClusterInfo.objects.create(
        cluster_id=12,
        cluster_name="test_es_2",
        cluster_type=models.ClusterInfo.TYPE_ES,
        domain_name="es_test.2",
        port=9090,
        description="",
        is_default_cluster=True,
        version="5.x",
    )
    models.ClusterInfo.objects.create(
        cluster_id=13,
        cluster_name="test_es_3",
        cluster_type=models.ClusterInfo.TYPE_ES,
        domain_name="es_test.3",
        port=9090,
        description="",
        is_default_cluster=True,
        version="5.x",
    )
    models.StorageClusterRecord.objects.create(
        table_id='1001_bklog.stdout', cluster_id=11, is_current=True, enable_time=base_time - timedelta(days=30)
    )
    models.StorageClusterRecord.objects.create(
        table_id='1001_bklog.stdout',
        cluster_id=12,
        is_current=False,
        enable_time=base_time - timedelta(days=60),
        disable_time=base_time - timedelta(days=30),
    )

    yield
    mocker.patch("bkmonitor.utils.consul.BKConsul", side_effect=consul_client)
    models.ESStorage.objects.all().delete()
    models.ClusterInfo.objects.all().delete()
    models.StorageClusterRecord.objects.all().delete()


@pytest.mark.django_db(databases=["default", "monitor_api"])
def test_compose_es_table_id_detail_v2(create_or_delete_records):
    client = SpaceTableIDRedis()

    enable_timestamp = int(
        models.StorageClusterRecord.objects.get(cluster_id=11, table_id='1001_bklog.stdout').enable_time.timestamp()
    )
    enable_timestamp_12 = int(
        models.StorageClusterRecord.objects.get(cluster_id=12, table_id='1001_bklog.stdout').enable_time.timestamp()
    )
    data = client._compose_es_table_id_detail(table_id_list=['1001_bklog.stdout'])
    # 构建 expected
    expected_json = {
        "storage_id": 11,
        "db": None,
        "measurement": "__default__",
        "source_type": "log",
        "options": {},
        "storage_type": "elasticsearch",
        "storage_cluster_records": [
            {"storage_id": 12, "enable_time": enable_timestamp_12},
            {"storage_id": 11, "enable_time": enable_timestamp},
        ],
    }
    expected = {'1001_bklog.stdout': json.dumps(expected_json)}
    assert data == expected
