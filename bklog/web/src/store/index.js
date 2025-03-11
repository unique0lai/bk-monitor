/* eslint-disable @typescript-eslint/no-misused-promises */
/*
 * Tencent is pleased to support the open source community by making
 * 蓝鲸智云PaaS平台 (BlueKing PaaS) available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * 蓝鲸智云PaaS平台 (BlueKing PaaS) is licensed under the MIT License.
 *
 * License for 蓝鲸智云PaaS平台 (BlueKing PaaS):
 *
 * ---------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

/**
 * @file main store
 * @author  <>
 */
import Vue, { set } from 'vue';

import {
  unifyObjectStyle,
  getOperatorKey,
  readBlobRespToJson,
  parseBigNumberList,
  setDefaultTableWidth,
  formatDate,
  getStorageIndexItem,
} from '@/common/util';
import { handleTransformToTimestamp } from '@/components/time-range/utils';
import Vuex from 'vuex';

import { deepClone } from '../components/monitor-echarts/utils';
import collect from './collect';
import { ConditionOperator } from './condition-operator';
import {
  IndexSetQueryResult,
  IndexFieldInfo,
  IndexItem,
  logSourceField,
  indexSetClusteringData,
  getDefaultRetrieveParams,
} from './default-values.ts';
import globals from './globals';
import RequestPool from './request-pool';
import retrieve from './retrieve';
import RouteUrlResolver from './url-resolver';
import { axiosInstance } from '@/api';
import http from '@/api';

Vue.use(Vuex);
const stateTpl = {
  userMeta: {}, // /meta/mine
  pageLoading: true,
  authDialogData: null,
  // 是否将unix时间戳格式化
  isFormatDate: true,
  // 当前运行环境
  runVersion: '',
  // 系统当前登录用户
  user: {},
  // 是否作为iframe被嵌套
  asIframe: false,
  iframeQuery: {},
  // 当前项目及Id
  space: {},
  spaceUid: '',
  indexId: '',
  indexItem: { ...IndexItem },
  operatorDictionary: {},
  /** 联合查询ID列表 */
  unionIndexList: [],
  /** 联合查询元素列表 */
  unionIndexItemList: [],

  // 收藏列表
  favoriteList: [],

  /** 索引集对应的字段列表信息 */
  // @ts-ignore
  indexFieldInfo: { ...IndexFieldInfo },
  indexSetQueryResult: { ...IndexSetQueryResult },
  indexSetFieldConfig: { clustering_config: { ...indexSetClusteringData } },
  indexSetFieldConfigList: {
    is_loading: false,
    data: [],
  },
  indexSetOperatorConfig: {
    /** 当前日志来源是否展示  用于字段更新后还保持显示状态 */
    isShowSourceField: false,
  },
  traceIndexId: '',
  // 业务Id
  bkBizId: '',
  // 我的项目列表
  mySpaceList: [],
  currentMenu: {},
  currentMenuItem: {},
  topMenu: [],
  menuList: [],
  visibleFields: [],
  // 数据接入权限
  menuProject: [],
  errorPage: ['notTraceIndex'],
  // 全局配置
  globalsData: {},
  activeTopMenu: {},
  activeManageNav: {},
  activeManageSubNav: {},
  // -- id, id对应数据
  collectDetail: [0, {}],
  showFieldsConfigPopoverNum: 0,
  showRouterLeaveTip: false,
  // 新人指引
  userGuideData: {},
  curCustomReport: null,
  // demo 业务链接
  demoUid: '',
  spaceBgColor: '', // 空间颜色
  isEnLanguage: false,
  chartSizeNum: 0, // 自定义上报详情拖拽后 表格chart需要自适应新宽度
  isExternal: false, // 外部版
  /** 是否展示全局脱敏弹窗 */
  isShowGlobalDialog: false,
  /** 当前全局设置弹窗的活跃id */
  globalActiveLabel: 'masking-setting', // masking-setting
  /** 全局设置列表 */
  globalSettingList: [],
  /** 日志灰度 */
  maskingToggle: {
    toggleString: 'off',
    toggleList: [],
  },
  /** 外部版路由菜单 */
  externalMenu: [],
  isAppFirstLoad: true,
  /** 是否清空了显示字段，展示全量字段 */
  isNotVisibleFieldsShow: false,
  showAlert: false, // 是否展示跑马灯
  isLimitExpandView: false,
  storeIsShowClusterStep: false,
  retrieveDropdownData: {},
  notTextTypeFields: [],
  tableLineIsWrap: false,
  tableJsonFormat: false,
  tableJsonFormatDepth: 1,
  tableShowRowIndex: false,
  // 是否展示空字段
  tableAllowEmptyField: false,
  isSetDefaultTableColumn: false,
  tookTime: 0,
  searchTotal: 0,
  showFieldAlias: localStorage.getItem('showFieldAlias') === 'true',
  clearSearchValueNum: 0,
  // 存放接口报错信息的对象
  apiErrorInfo: {},
  clusterParams: null,
};

const store = new Vuex.Store({
  // 模块
  modules: {
    retrieve,
    collect,
    globals,
  },
  // 公共 store
  state: deepClone(stateTpl),
  // 公共 getters
  getters: {
    runVersion: state => state.runVersion,
    user: state => state.user,
    space: state => state.space,
    spaceUid: state => state.spaceUid,
    indexId: state => state.indexId,
    visibleFields: state => state.visibleFields,
    /** 是否是联合查询 */
    isUnionSearch: state => !!state.unionIndexList.length,
    /** 联合查询索引集ID数组 */
    unionIndexList: state => state.unionIndexList,
    unionIndexItemList: state => state.unionIndexItemList,
    traceIndexId: state => state.traceIndexId,
    bkBizId: state => state.bkBizId,
    mySpaceList: state => state.mySpaceList,
    pageLoading: state => state.pageLoading,
    globalsData: state => state.globalsData,
    // -- 返回数据
    collectDetail: state => state.collectDetail[1],
    asIframe: state => state.asIframe,
    iframeQuery: state => state.iframeQuery,
    demoUid: state => state.demoUid,
    accessUserManage: state =>
      Boolean(
        state.topMenu
          .find(item => item.id === 'manage')
          ?.children.some(item => item.id === 'permissionGroup' && item.project_manage === true),
      ),
    spaceBgColor: state => state.spaceBgColor,
    isEnLanguage: state => state.isEnLanguage,
    chartSizeNum: state => state.chartSizeNum,
    isShowGlobalDialog: state => state.isShowGlobalDialog,
    globalActiveLabel: state => state.globalActiveLabel,
    globalSettingList: state => state.globalSettingList,
    maskingToggle: state => state.maskingToggle,
    isNotVisibleFieldsShow: state => state.isNotVisibleFieldsShow,
    /** 脱敏灰度判断 */
    isShowMaskingTemplate: state =>
      state.maskingToggle.toggleString === 'on' || state.maskingToggle.toggleList.includes(Number(state.bkBizId)),
    isLimitExpandView: state => state.isLimitExpandView,
    common_filter_addition: state => state.retrieve.catchFieldCustomConfig.filterAddition ?? [],
    // @ts-ignore
    retrieveParams: state => {
      const {
        start_time,
        end_time,
        addition,
        begin,
        size,
        keyword = '*',
        ip_chooser,
        host_scopes,
        interval,
        search_mode,
        sort_list,
        format
      } = state.indexItem;

      const filterAddition = addition
        .filter(item => !item.disabled && item.field !== '_ip-select_')
        .map(({ field, operator, value }) => {
          const addition = {
            field,
            operator,
            value,
          };

          if (['is true', 'is false'].includes(addition.operator)) {
            addition.value = [''];
          }

          return addition;
        });

      const searchParams =
        search_mode === 'sql' ? { keyword, addition: [] } : { addition: filterAddition, keyword: '*' };

      if (searchParams.keyword.replace(/\s*/, '') === '') {
        searchParams.keyword = '*';
      }

      return {
        start_time,
        end_time,
        format,
        addition: filterAddition,
        begin,
        size,
        ip_chooser,
        host_scopes,
        interval,
        search_mode,
        sort_list,
        bk_biz_id: state.bkBizId,
        ...searchParams,
      };
    },
    isNewRetrieveRoute: () => {
      const v = localStorage.getItem('retrieve_version') ?? 'v2';
      return v === 'v2';
    },
    storeIsShowClusterStep: state => state.storeIsShowClusterStep,
    getApiError: state => apiName => {
      return state.apiErrorInfo[apiName];
    },
    resultTableStaticWidth: state => {
      return (state.indexSetOperatorConfig?.bcsWebConsole?.is_active ? 84 : 58) + 50;
    },
  },
  // 公共 mutations
  mutations: {
    updatetableJsonFormatDepth(state, val) {
      state.tableJsonFormatDepth = val;
    },
    updateTableJsonFormat(state, val) {
      state.tableJsonFormat = val;
    },
    updateTableShowRowIndex(state, val) {
      state.tableShowRowIndex = val;
    },
    // 更新是否展示空字段
    updateTableEmptyFieldFormat(state, val) {
      state.tableAllowEmptyField = val;
    },
    updateApiError(state, { apiName, errorMessage }) {
      Vue.set(state.apiErrorInfo, apiName, errorMessage);
    },
    deleteApiError(state, apiName) {
      Vue.delete(state.apiErrorInfo, apiName);
    },
    updateFavoriteList(state, payload) {
      state.favoriteList.length = 0;
      state.favoriteList = [];
      state.favoriteList.push(...(payload ?? []));
    },
    updateChartParams(state, params) {
      Object.keys(params).forEach(key => {
        if (Array.isArray(state.indexItem.chart_params[key])) {
          state.indexItem.chart_params[key].splice(0, state.indexItem.chart_params[key].length, ...(params[key] ?? []));
        } else {
          set(state.indexItem.chart_params, key, params[key]);
        }
      });
    },
    updateIndexItem(state, payload) {
      ['ids', 'items', 'catchUnionBeginList'].forEach(key => {
        if (Array.isArray(state.indexItem[key]) && Array.isArray(payload?.[key] ?? false)) {
          state.indexItem[key].splice(
            0,
            state.indexItem[key].length,
            ...(payload?.[key] ?? []).filter(v => v !== null && v !== undefined),
          );
        }
      });

      Object.assign(state.indexItem, payload ?? {});
    },

    updateIndexSetOperatorConfig(state, payload) {
      Object.keys(payload ?? {}).forEach(key => {
        set(state.indexSetOperatorConfig, key, payload[key]);
      });
    },

    /**
     * 当切换索引集时，重置请求参数默认值
     * @param {*} state
     * @param {*} payload
     */
    resetIndexsetItemParams(state, payload) {
      const defaultValue = { ...getDefaultRetrieveParams(), isUnionIndex: false, selectIsUnionSearch: false };
      ['ids', 'items', 'catchUnionBeginList'].forEach(key => {
        if (Array.isArray(state.indexItem[key])) {
          state.indexItem[key].splice(
            0,
            state.indexItem[key].length,
            ...(payload?.[key] ?? []).filter(v => v !== null && v !== undefined),
          );
        }
      });

      state.indexItem.isUnionIndex = false;
      state.unionIndexList.splice(0, state.unionIndexList.length);
      state.indexItem.chart_params = deepClone(IndexItem.chart_params);

      if (payload?.addition?.length >= 0) {
        state.indexItem.addition.splice(
          0,
          state.indexItem.addition.length,
          ...payload?.addition.map(item => {
            const instance = new ConditionOperator(item);
            return { ...item, ...instance.getRequestParam() };
          }),
        );
      }

      const copyValue = Object.keys(payload ?? {}).reduce((result, key) => {
        if (!['ids', 'items', 'catchUnionBeginList', 'addition'].includes(key)) {
          Object.assign(result, { [key]: payload[key] });
        }

        return result;
      }, {});
      Object.assign(state.indexItem, defaultValue, copyValue);
    },

    updateIndexSetFieldConfig(state, payload) {
      const defVal = { ...indexSetClusteringData };
      const { config } = payload ?? { config: [] };
      const result = (config ?? []).reduce((output, item) => Object.assign(output, { [item.name]: { ...item } }), {
        clustering_config: defVal,
      });

      Object.assign(state.indexSetFieldConfig, result ?? {});
    },

    resetIndexSetQueryResult(state, payload) {
      Object.assign(state.indexSetQueryResult, IndexSetQueryResult, payload ?? {});
    },

    updateIndexSetQueryResult(state, payload) {
      Object.assign(state.indexSetQueryResult, payload ?? {});
    },

    updateIndexItemParams(state, payload) {
      if (payload?.addition?.length >= 0) {
        state.indexItem.addition.splice(
          0,
          state.indexItem.addition.length,
          ...payload?.addition.map(item => {
            const instance = new ConditionOperator(item);
            return { ...item, ...instance.getRequestParam() };
          }),
        );
      }

      const copyValue = Object.keys(payload ?? {}).reduce((result, key) => {
        if (!['addition'].includes(key)) {
          Object.assign(result, { [key]: payload[key] });
        }

        return result;
      }, {});

      Object.assign(state.indexItem, copyValue ?? {});
    },

    updateIndexSetFieldConfigList() {
      if (payload.is_loading !== undefined) {
        state.indexSetFieldConfigList.is_loading = payload.is_loading;
      }

      if (payload.data) {
        state.indexSetFieldConfigList.data.length = 0;
        state.indexSetFieldConfigList.data.push(...(payload ?? []));
      }
    },

    updateAddition(state) {
      state.indexItem.addition.forEach(item => {
        const instance = new ConditionOperator(item);
        Object.assign(item, instance.getRequestParam());
      });
    },

    updataOperatorDictionary(state, payload) {
      state.operatorDictionary = {};
      (payload.fields ?? []).forEach(field => {
        const { field_operator = [] } = field;
        field_operator.forEach(item => {
          const { operator } = item;
          const key = getOperatorKey(operator);
          Object.assign(state.operatorDictionary, { [key]: item });
        });
      });
    },

    updateUserMeta(state, payload) {
      state.userMeta = payload;
    },
    /**
     * 设置初始化 loading 是否显示
     */
    setPageLoading(state, loading) {
      state.pageLoading = loading;
    },
    updateAuthDialogData(state, payload) {
      state.authDialogData = payload;
    },
    updateIsFormatDate(state, payload) {
      state.isFormatDate = payload;
    },
    /**
     * 更新当前运行环境
     * @param {Object} state store state
     * @param {String} runVersion 运行环境
     */
    updateRunVersion(state, runVersion) {
      state.runVersion = runVersion;
    },
    /**
     * 更新当前用户 user
     *
     * @param {Object} state store state
     * @param {Object} user user 对象
     */
    updateUser(state, user) {
      state.user = Object.assign({}, user);
    },
    /**
     * 更新当前路由对应导航
     */
    updateCurrentMenu(state, current) {
      Vue.set(state, 'currentMenu', current);
    },
    updateCurrentMenuItem(state, item) {
      Vue.set(state, 'currentMenuItem', item);
    },
    updateSpace(state, spaceUid) {
      state.space = state.mySpaceList.find(item => item.space_uid === spaceUid) || {};
      state.bkBizId = state.space.bk_biz_id;
      state.spaceUid = spaceUid;
      state.isSetDefaultTableColumn = false;
    },
    updateMySpaceList(state, spaceList) {
      state.mySpaceList = spaceList.map(item => {
        const defaultTag = { id: item.space_type_id, name: item.space_type_name, type: item.space_type_id };
        return {
          ...item,
          name: item.space_name.replace(/\[.*?\]/, ''),
          py_text: Vue.prototype.$bkToPinyin(item.space_name, true),
          tags:
            item.space_type_id === 'bkci' && item.space_code
              ? [defaultTag, { id: 'bcs', name: window.mainComponent.$t('容器项目'), type: 'bcs' }]
              : [defaultTag],
        };
      });
    },
    updateIndexId(state, indexId) {
      state.indexId = indexId;
    },
    updateUnionIndexList(state, unionIndexList) {
      state.unionIndexList.splice(
        0,
        state.unionIndexList.length,
        ...unionIndexList.filter(v => v !== null && v !== undefined),
      );
      state.indexItem.ids.splice(
        0,
        state.indexItem.ids.length,
        ...unionIndexList.filter(v => v !== null && v !== undefined),
      );
      const unionIndexItemList = state.retrieve.indexSetList.filter(item => unionIndexList.includes(item.index_set_id));
      state.unionIndexItemList.splice(0, state.unionIndexItemList.length, ...unionIndexItemList);
    },
    updateUnionIndexItemList(state, unionIndexItemList) {
      state.unionIndexItemList = unionIndexItemList;
    },
    updateTraceIndexId(state, indexId) {
      state.traceIndexId = indexId;
    },
    updateMenuList(state, menuList) {
      state.menuList.splice(0, state.menuList.length, ...menuList);
    },
    updateActiveTopMenu(state, payload) {
      state.activeTopMenu = payload;
    },
    updateActiveManageNav(state, payload) {
      state.activeManageNav = payload;
    },
    updateActiveManageSubNav(state, payload) {
      state.activeManageSubNav = payload;
    },
    updateMenuProject(state, menuProject) {
      state.menuProject.splice(0, state.menuProject.length, ...menuProject);
    },
    updateTopMenu(state, topMenu) {
      state.topMenu.splice(0, state.topMenu.length, ...topMenu);
    },
    updateGlobalsData(state, globalsData) {
      state.globalsData = globalsData;
      Vue.set(state, 'globalsData', globalsData);
    },
    // -- 代码调整 collectDetail: [id, 数据]
    updateCollectDetail(state, collectDetail) {
      const data = collectDetail[1];
      data.params.paths = data.params.paths.map(item => ({ value: item }));
      state.collectDetail = data;
    },
    updateAsIframe(state, asIframe) {
      state.asIframe = asIframe;
    },
    updateIframeQuery(state, iframeQuery) {
      Object.assign(state.iframeQuery, iframeQuery);
    },
    updateShowFieldsConfigPopoverNum(state, showFieldsConfigPopoverNum) {
      state.showFieldsConfigPopoverNum += showFieldsConfigPopoverNum;
    },
    updateRouterLeaveTip(state, isShow) {
      state.showRouterLeaveTip = isShow;
    },
    setUserGuideData(state, userGuideData) {
      state.userGuideData = userGuideData;
    },
    setDemoUid(state, demoUid) {
      state.demoUid = demoUid;
    },
    setSpaceBgColor(state, val) {
      state.spaceBgColor = val;
    },
    updateIsEnLanguage(state, val) {
      state.isEnLanguage = val;
    },
    updateChartSize(state) {
      state.chartSizeNum += 1;
    },
    updateIsShowGlobalDialog(state, val) {
      state.isShowGlobalDialog = val;
    },
    updateGlobalActiveLabel(state, val) {
      state.globalActiveLabel = val;
    },
    updateGlobalSettingList(state, val) {
      state.globalSettingList = val;
    },
    updateMaskingToggle(state, val) {
      state.maskingToggle = val;
    },
    updateExternalMenu(state, val) {
      state.externalMenu = val;
    },
    updateVisibleFields(state, val) {
      state.visibleFields.splice(0, state.visibleFields.length, ...(val ?? []));
      state.indexFieldInfo.request_counter++;
    },
    updateVisibleFieldMinWidth(state, tableList, fieldList) {
      const staticWidth = state.indexSetOperatorConfig?.bcsWebConsole?.is_active ? 84 : 58 + 50;
      setDefaultTableWidth(fieldList ?? state.visibleFields, tableList, null, staticWidth);
    },
    updateIsNotVisibleFieldsShow(state, val) {
      state.isNotVisibleFieldsShow = val;
    },
    updateNoticeAlert(state, val) {
      state.showAlert = val;
    },
    updateIsLimitExpandView(state, val) {
      localStorage.setItem('EXPAND_SEARCH_VIEW', JSON.stringify(val));
      state.isLimitExpandView = val;
    },
    updateIndexFieldInfo(state, payload) {
      Object.assign(state.indexFieldInfo, payload ?? {});
    },
    updateIndexFieldEggsItems(state, payload) {
      const { start_time, end_time } = state.indexItem;
      const lastQueryTimerange = `${start_time}_${end_time}`;
      Object.keys(payload ?? {}).forEach(key => {
        set(state.indexFieldInfo.aggs_items, key, payload[key]);
      });
      state.indexFieldInfo.last_eggs_request_token = lastQueryTimerange;
    },
    resetIndexFieldInfo(state, payload) {
      const defValue = { ...IndexFieldInfo };
      state.indexFieldInfo = Object.assign(defValue, payload ?? {});
    },
    updateStoreIsShowClusterStep(state, val) {
      state.storeIsShowClusterStep = val;
    },
    updateClusterParams(state, payload) {
      state.clusterParams = payload;
    },
    updateSqlQueryFieldList(state, payload) {
      const target = {};
      state.retrieveDropdownData = {};

      const recursiveIncreaseData = (dataItem, prefixFieldKey = '') => {
        dataItem &&
          Object.entries(dataItem).forEach(([field, value]) => {
            if (typeof value === 'object') {
              recursiveIncreaseData(value, `${prefixFieldKey + field}.`);
            } else {
              const fullFieldKey = prefixFieldKey ? prefixFieldKey + field : field;
              let fieldData = target[fullFieldKey];
              if (fieldData) fieldData.__totalCount += 1;
              if (value || value === 0) {
                if (!fieldData) {
                  Object.assign(target, {
                    [fullFieldKey]: Object.defineProperties(
                      {},
                      {
                        __fieldType: {
                          // 该字段下的值的数据类型，可能是数值、字符串、布尔值
                          value: typeof value,
                        },
                        __totalCount: {
                          // 总记录数量
                          value: 1,
                          writable: true,
                        },
                        __validCount: {
                          // 有效值数量
                          value: 0,
                          writable: true,
                        },
                      },
                    ),
                  });
                  fieldData = target[fullFieldKey];
                }
                fieldData.__validCount += 1;
                fieldData[value] += 1;
                if (state.notTextTypeFields.includes(field) && !fieldData?.[value]) {
                  // 非 text 类型字段统计可选值，text 则由用户手动输入
                  fieldData[value] = 1;
                }
              }
            }
          });
      };

      // 更新下拉字段可选值信息
      const computeRetrieveDropdownData = listData => {
        listData.forEach(dataItem => {
          recursiveIncreaseData(dataItem);
        });
      };

      computeRetrieveDropdownData(payload ?? []);

      Object.keys(target).forEach(key => {
        Vue.set(state.retrieveDropdownData, key, target[key]);
      });
    },
    updateNotTextTypeFields(state, payload) {
      state.notTextTypeFields.length = [];
      state.notTextTypeFields = [];

      state.notTextTypeFields.push(
        ...(payload.fields ?? []).filter(field => field.field_type !== 'text').map(item => item.field_name),
      );
    },
    updateTableLineIsWrap(state, payload) {
      state.tableLineIsWrap = payload;
    },
    updateShowFieldAlias(state, payload) {
      window.localStorage.setItem('showFieldAlias', payload);
      state.showFieldAlias = payload;
    },
    /** 初始化表格宽度 为false的时候会按照初始化的情况来更新宽度 */
    updateIsSetDefaultTableColumn(state, payload) {
      // 如果浏览器记录过当前索引集表格拖动过 则不需要重新计算
      if (!state.isSetDefaultTableColumn) {
        const catchFieldsWidthObj = store.state.retrieve.catchFieldCustomConfig.fieldsWidth;
        const staticWidth = state.indexSetOperatorConfig?.bcsWebConsole?.is_active ? 84 : 58;
        setDefaultTableWidth(
          state.visibleFields,
          payload?.list ?? state.indexSetQueryResult.list,
          catchFieldsWidthObj,
          staticWidth + 60,
        );
        // request_counter 用于触发查询结果表格的更新
        state.indexFieldInfo.request_counter++;
      }
      if (typeof payload === 'boolean') state.isSetDefaultTableColumn = payload;
    },
    /**
     * @desc: 用于更新可见field
     * 根据传入的 `payload` 参数更新当前可见的字段。`payload` 可以是一个字段名称的数组，
     * 或者是包含字段名称数组和版本信息的对象。
     *
     * @param {Array | Object} payload  - 可传入字段名称数组或包含字段数组以及版本信息的对象。
     *   - 当为数组时，表示字段名称列表。
     *   - 当为对象时，应包含以下属性：
     *     - {Array} displayFieldNames - 字段名称数组。
     *     - {string} version - 版本信息，包含 v2时，表示是新版本设计，目前包含了object字段层级展示的添加功能，后续如果需要区别于之前的逻辑处理，可以参照此逻辑处理(暂不生效)
     *
     */
    resetVisibleFields(state, payload) {
      const isVersion2Payload = payload?.version === 'v2';
      const catchDisplayFields = store.state.retrieve.catchFieldCustomConfig.displayFields;
      const displayFields = catchDisplayFields.length ? catchDisplayFields : null;
      // 请求字段时 判断当前索引集是否有更改过字段 若更改过字段则使用session缓存的字段显示
      const filterList =
        (isVersion2Payload ? payload.displayFieldNames : payload || displayFields) ??
        state.indexFieldInfo.display_fields;
      const visibleFields =
        filterList
          .map(displayName => {
            const field = state.indexFieldInfo.fields.find(field => field.field_name === displayName);
            if (field) return field;
            return {
              field_type: 'object',
              field_name: displayName,
              field_alias: '',
              is_display: false,
              is_editable: true,
              tag: '',
              origin_field: '',
              es_doc_values: true,
              is_analyzed: true,
              is_virtual_obj_node: true,
              field_operator: [],
              is_built_in: true,
              is_case_sensitive: false,
              tokenize_on_chars: '',
              description: '',
              filterVisible: true,
            };
          })
          .filter(Boolean) ?? [];
      store.commit('updateVisibleFields', visibleFields);
      store.commit('updateIsNotVisibleFieldsShow', !visibleFields.length);

      if (state.indexItem.isUnionIndex) store.dispatch('showShowUnionSource', { keepLastTime: true });
    },
    resetIndexSetOperatorConfig(state) {
      const {
        bkmonitor,
        context_and_realtime: contextAndRealtime,
        bcs_web_console: bcsWebConsole,
      } = state.indexSetFieldConfig;
      // 字段设置的参数传到实时日志和上下文
      let indexSetValue;
      if (!state.indexItem.isUnionIndex) {
        const item = state.indexItem.items[0];
        indexSetValue = {
          scenarioID: item?.scenario_id,
          sortFields: item?.sort_fields ?? [],
          targetFields: item?.target_fields ?? [],
        };
      } else {
        indexSetValue = {};
      }
      store.commit('updateIndexSetOperatorConfig', {
        bkmonitor,
        bcsWebConsole,
        contextAndRealtime,
        indexSetValue,
        toolMessage: {
          webConsole: bcsWebConsole.is_active ? 'WebConsole' : bcsWebConsole?.extra?.reason,
          realTimeLog: contextAndRealtime.is_active
            ? window.mainComponent.$t('实时日志')
            : contextAndRealtime?.extra?.reason,
          contextLog: contextAndRealtime.is_active
            ? window.mainComponent.$t('上下文')
            : contextAndRealtime?.extra?.reason,
        },
      });
    },
    updateClearSearchValueNum(state, payload) {
      state.clearSearchValueNum = payload;
    },
    // 初始化监控默认数据
    initMonitorState(state, payload) {
      Object.assign(state, payload);
    },
    resetState(state) {
      Object.assign(state, deepClone(stateTpl));
    },
  },
  actions: {
    /**
     * 获取用户信息
     *
     * @param {Function} commit store commit mutation handler
     * @param {Object} state store state
     * @param {Function} dispatch store dispatch action handler
     * @param {Object} params 请求参数
     * @param {Object} config 请求的配置
     *
     * @return {Promise} promise 对象
     */
    userInfo({ commit }, params, config = {}) {
      return http.request('userInfo/getUserInfo', { query: params, config }).then(response => {
        const userData = response.data || {};
        commit('updateUser', userData);
        return userData;
      });
    },

    /**
     * 获取菜单列表
     *
     * @param {Function} commit store commit mutation handler
     * @param {Object} state store state
     * @param {Function} dispatch store dispatch action handler
     * @param {Object} params 请求参数
     * @param {Object} config 请求的配置
     *
     * @return {Promise} promise 对象
     */
    getMenuList({}, spaceUid) {
      return http.request('meta/menu', {
        query: {
          space_uid: spaceUid,
        },
      });
    },
    getGlobalsData({ commit }) {
      return http.request('collect/globals', { query: {} }).then(response => {
        const globalsData = response.data || {};
        commit('updateGlobalsData', globalsData);
        return globalsData;
      });
    },
    // -- 代码调整
    getCollectDetail({ commit, state }, data) {
      // 判断是否有该id的缓存数据
      if (state.collectDetail[0] !== data.collector_config_id) {
        commit('updateCollectDetail', [data.collector_config_id, data || {}]);
        return data;
      }
    },
    // 判断有无权限
    checkAllowed(context, paramData) {
      return new Promise(async (resolve, reject) => {
        try {
          const checkRes = await http.request('auth/checkAllowed', {
            data: paramData,
          });
          for (const item of checkRes.data) {
            if (item.is_allowed === false) {
              // 无权限
              resolve({
                isAllowed: false,
              });
              return;
            }
          }
          // 有权限
          resolve({
            isAllowed: true,
          });
        } catch (err) {
          // 请求出错
          reject(err);
        }
      });
    },
    // 已知无权限，需要获取信息
    getApplyData(context, paramData) {
      return http.request('auth/getApplyData', {
        data: paramData,
      });
    },
    // 判断有无权限，无权限获取相关信息
    checkAndGetData(context, paramData) {
      return new Promise(async (resolve, reject) => {
        try {
          const checkRes = await http.request('auth/checkAllowed', {
            data: paramData,
          });
          for (const item of checkRes.data) {
            if (item.is_allowed === false) {
              // 无权限
              const applyDataRes = await http.request('auth/getApplyData', {
                data: paramData,
              });
              resolve({
                isAllowed: false,
                data: applyDataRes.data,
              });
              return;
            }
          }
          // 有权限
          resolve({
            isAllowed: true,
          });
        } catch (err) {
          // 请求出错
          reject(err);
        }
      });
    },

    /**
     * 初始化时，通过路由参数和请求返回的索引集列表初始化索引集默认选中值
     * @param {*} param0
     * @param {*} param1
     */
    updateIndexItemByRoute({ commit, state }, { route, list = [] }) {
      const ids = [];
      let isUnionIndex = false;
      commit('resetIndexSetQueryResult', { search_count: 0 });
      const resolver = new RouteUrlResolver({ route });
      const result = resolver.convertQueryToStore();

      if ((result?.unionList?.length ?? 0) > 0) {
        isUnionIndex = true;
        ids.push(...result?.unionList);
        commit('updateUnionIndexList', ids);
      } else {
        const indexId = window.__IS_MONITOR_COMPONENT__ ? route.query.indexId : route.params.indexId;
        if (indexId) {
          ids.push(indexId);
        }
      }

      if (!isUnionIndex && !ids.length && list?.length) {
        ids.push(getStorageIndexItem(list));
      }

      if (route.query?.bizId) {
        state.bkBizId = route.query?.bizId;
      }

      if (result.ip_chooser) {
        const ipSelectValue = result.addition?.find(c => c.field === '_ip-select_');
        if (ipSelectValue) {
          ipSelectValue.value = [result.ip_chooser];
        } else {
          if (!result.addition) result.addition = [];

          if (Object.keys(result.ip_chooser ?? {}).length) {
            result.addition.push({
              field: '_ip-select_',
              operator: '',
              value: [result.ip_chooser],
            });
          }
        }
      }

      if (result.clusterParams) {
        commit('updateClusterParams', result.clusterParams);
      }

      if (ids.length) {
        delete result.unionList;
        delete result.clusterParams;
        const payload = {
          ...result,
          ids,
          selectIsUnionSearch: isUnionIndex,
          chart_params: deepClone(IndexItem.chart_params),
          items: ids.map(val => (list || []).find(item => item.index_set_id === val)).filter(val => val !== undefined),
          isUnionIndex,
        };
        
        if (payload.items.length === 1 && !payload.keyword && !payload.addition?.length) {
          if (payload.items[0].query_string) {
            payload.keyword = payload.items[0].query_string;
            payload.search_mode = 'sql';
            payload.addition = [];
          } else if (payload.items[0].addition) {
            payload.addition = payload.items[0].addition;
            payload.search_mode = 'ui';
            payload.keyword = '';
          }
        }

        commit('updateIndexId', isUnionIndex ? undefined : ids[0]);
        commit('updateIndexItem', payload);
      }
    },

    /** 请求字段config信息 */
    requestIndexSetFieldInfo({ commit, state }) {
      // @ts-ignore
      const { ids = [], start_time = '', end_time = '', isUnionIndex } = state.indexItem;

      commit('resetIndexFieldInfo');
      commit('updataOperatorDictionary', {});
      commit('updateNotTextTypeFields', {});
      commit('updateIndexSetFieldConfig', {});
      commit('updateVisibleFields', []);

      if (!ids.length) {
        return;
      }
      commit('resetIndexFieldInfo', { is_loading: true });
      const urlStr = isUnionIndex ? 'unionSearch/unionMapping' : 'retrieve/getLogTableHead';
      !isUnionIndex && commit('deleteApiError', urlStr);
      const queryData = {
        start_time,
        end_time,
        is_realtime: 'True',
      };
      if (isUnionIndex) {
        Object.assign(queryData, {
          index_set_ids: ids,
        });
      }

      return http
        .request(
          urlStr,
          {
            params: { index_set_id: ids[0] },
            query: !isUnionIndex ? queryData : undefined,
            data: isUnionIndex ? queryData : undefined,
          },
          isUnionIndex ? {} : { catchIsShowMessage: false },
        )
        .then(res => {
          commit('updateIndexFieldInfo', res.data ?? {});
          commit('updataOperatorDictionary', res.data ?? {});
          commit('updateNotTextTypeFields', res.data ?? {});
          commit('updateIndexSetFieldConfig', res.data ?? {});
          commit('retrieve/updateFiledSettingConfigID', res.data?.config_id ?? -1); // 当前字段配置configID
          commit('retrieve/updateCatchFieldCustomConfig', res.data.user_custom_config); // 更新用户个人配置
          commit('resetVisibleFields');
          commit('resetIndexSetOperatorConfig');
          commit('updateIsSetDefaultTableColumn');
          return res;
        })
        .catch(err => {
          !isUnionIndex && commit('updateApiError', { apiName: urlStr, errorMessage: err });
          commit('updateIndexFieldInfo', { is_loading: false });
        })
        .finally(() => {
          commit('updateIndexFieldInfo', { is_loading: false });
        });
    },

    /** 请求获取用户个人配置信息 */
    requestIndexSetCustomConfigInfo({ commit, state }) {
      // @ts-ignore
      const { ids = [], start_time = '', end_time = '', isUnionIndex } = state.indexItem;
      if (!ids.length) {
        return;
      }
      const urlStr = isUnionIndex ? 'unionSearch/unionMapping' : 'retrieve/getLogTableHead';
      !isUnionIndex && commit('deleteApiError', urlStr);
      const queryData = {
        start_time,
        end_time,
        is_realtime: 'True',
      };
      if (isUnionIndex) {
        Object.assign(queryData, {
          index_set_ids: ids,
        });
      }
      return http
        .request(
          urlStr,
          {
            params: { index_set_id: ids[0] },
            query: !isUnionIndex ? queryData : undefined,
            data: isUnionIndex ? queryData : undefined,
          },
          isUnionIndex ? {} : { catchIsShowMessage: false },
        )
        .then(res => {
          commit('retrieve/updateCatchFieldCustomConfig', res.data.user_custom_config); // 更新用户个人配置
          return res;
        })
        .catch(() => {
          commit('retrieve/updateCatchFieldCustomConfig', {
            ...state.retrieve.catchFieldCustomConfig,
            filterSetting: {},
          });
        })
        .finally();
    },
    /**
     * 执行查询
     */
    requestIndexSetQuery(
      { commit, state, getters, dispatch },
      payload = { isPagination: false, cancelToken: null, searchCount: undefined, formChartChange: true },
    ) {
      if (
        (!state.indexItem.isUnionIndex && !state.indexId) ||
        (state.indexItem.isUnionIndex && !state.indexItem.ids.length)
      ) {
        state.searchTotal = 0;
        commit('updateSqlQueryFieldList', []);
        commit('updateIndexSetQueryResult', { is_error: false, exception_msg: '' });
        return; // Promise.reject({ message: `index_set_id is undefined` });
      }
      let begin = state.indexItem.begin;
      const { size, format, ...otherPrams } = getters.retrieveParams;

      // 每次请求这里需要根据选择日期时间这里计算最新的timestamp
      // 最新的 start_time, end_time 也要记录下来，用于字段统计时，保证请求的参数一致
      const { datePickerValue } = state.indexItem;
      const letterRegex = /[a-zA-Z]/;
      const needTransform = datePickerValue.every(d => letterRegex.test(d));

      const [start_time, end_time] = needTransform
        ? handleTransformToTimestamp(datePickerValue, format)
        : [state.indexItem.start_time, state.indexItem.end_time];

      if (needTransform) {
        commit('updateIndexItem', { start_time, end_time });
      }

      if (!payload?.isPagination && payload.formChartChange) {
        store.commit('retrieve/updateChartKey');
      }
      const searchCount = payload.searchCount ?? state.indexSetQueryResult.search_count + 1;
      commit(payload.isPagination ? 'updateIndexSetQueryResult' : 'resetIndexSetQueryResult', {
        is_loading: true,
        search_count: searchCount,
      });

      const baseUrl = process.env.NODE_ENV === 'development' ? 'api/v1' : window.AJAX_URL_PREFIX;
      const cancelTokenKey = 'requestIndexSetQueryCancelToken';
      RequestPool.execCanceToken(cancelTokenKey);
      const requestCancelToken = payload.cancelToken ?? RequestPool.getCancelToken(cancelTokenKey);

      // 区分联合查询和单选查询
      const searchUrl = !state.indexItem.isUnionIndex
        ? `/search/index_set/${state.indexId}/search/`
        : '/search/index_set/union_search/';

      const baseData = {
        bk_biz_id: state.bkBizId,
        size,
        ...otherPrams,
        start_time,
        end_time,
        addition: [...otherPrams.addition, ...(getters.common_filter_addition ?? [])],
      };

      // 更新联合查询的begin
      const unionConfigs = state.unionIndexList.map(item => ({
        begin: payload.isPagination
          ? state.indexItem.catchUnionBeginList.find(cItem => String(cItem?.index_set_id) === item)?.begin ?? 0
          : 0,
        index_set_id: item,
      }));

      const queryBegin = payload.isPagination ? (begin += size) : 0;

      const queryData = Object.assign(
        baseData,
        !state.indexItem.isUnionIndex
          ? {
              begin: queryBegin, // 单选检索的begin
            }
          : {
              union_configs: unionConfigs,
            },
      );
      const params = {
        method: 'post',
        url: searchUrl,
        cancelToken: requestCancelToken,
        withCredentials: true,
        baseURL: baseUrl,
        responseType: 'blob',
        data: queryData,
      };
      if (state.isExternal) {
        params.headers = {
          'X-Bk-Space-Uid': state.spaceUid,
        };
      }

      return axiosInstance(params)
        .then(resp => {
          if (resp.data && !resp.message) {
            return readBlobRespToJson(resp.data).then(({ code, data, result, message }) => {
              const rsolvedData = data;
              if (result) {
                const indexSetQueryResult = state.indexSetQueryResult;
                const logList = parseBigNumberList(rsolvedData.list);
                const originLogList = parseBigNumberList(rsolvedData.origin_log_list);

                rsolvedData.list = payload.isPagination ? indexSetQueryResult.list.concat(logList) : logList;
                rsolvedData.origin_log_list = payload.isPagination
                  ? indexSetQueryResult.origin_log_list.concat(originLogList)
                  : originLogList;

                const catchUnionBeginList = parseBigNumberList(rsolvedData?.union_configs || []);
                state.tookTime = payload.isPagination
                  ? state.tookTime + Number(data?.took || 0)
                  : Number(data?.took || 0);

                if (!payload?.isPagination) {
                  commit('updateIsSetDefaultTableColumn', { list: logList });
                  dispatch('requestSearchTotal');
                }
                // 更新页数
                commit('updateSqlQueryFieldList', logList);
                commit('updateIndexItem', { catchUnionBeginList, begin: payload.isPagination ? begin : 0 });
                commit('updateIndexSetQueryResult', rsolvedData);

                return {
                  data,
                  message,
                  code,
                  result,
                  length: logList.length,
                };
              }

              commit('updateIndexSetQueryResult', { exception_msg: message, is_error: !result });

              return {
                data,
                message,
                code,
                result,
                length: 0,
              };
            });
          }

          return { data, message, result: false };
        })
        .catch(e => {
          state.searchTotal = 0;
          commit('updateSqlQueryFieldList', []);
          commit('updateIndexSetQueryResult', { is_error: true, exception_msg: e?.message ?? e?.toString() });
        })
        .finally(() => {
          commit('updateIndexSetQueryResult', { is_loading: false });
        });
    },

    requestFieldConfigList({ state, commit }, payload) {
      const cancelTokenKey = 'requestFieldConfigCancelToken';
      RequestPool.execCanceToken(cancelTokenKey);
      const requestCancelToken = payload.cancelToken ?? RequestPool.getCancelToken(cancelTokenKey);
      commit('updateIndexSetFieldConfigList', {
        data: [],
        is_loading: true,
      });
      return http
        .request(
          'retrieve/getFieldsListConfig',
          {
            data: {
              ...(state.indexItem.isUnionIndex
                ? { index_set_ids: state.unionIndexList }
                : { index_set_id: state.indexId }),
              scope: 'default',
              index_set_type: state.indexItem.isUnionIndex ? 'union' : 'single',
            },
          },
          {
            cancelToken: requestCancelToken,
          },
        )
        .then(resp => {
          commit('updateIndexSetFieldConfigList', {
            data: resp.data ?? [],
          });

          return resp;
        })
        .finally(() => {
          commit('updateIndexSetFieldConfigList', {
            is_loading: false,
          });
        });
    },

    /**
     * 索引集选择改变事件
     * 更新索引集相关缓存 & 发起当前索引集所需字段信息请求
     * @param {*} param0
     * @param {*} payload
     */
    requestIndexSetItemChanged({ commit, dispatch }, payload) {
      commit('updateIndexItem', payload);
      commit('resetIndexSetQueryResult', { search_count: 0, is_loading: true });

      if (!payload.isUnionIndex) {
        commit('updateIndexId', payload.ids[0]);
      }

      return dispatch('requestIndexSetFieldInfo');
    },

    /**
     * 请求提示词列表
     * @param {*} param0
     * @param {*} payload: { force: boolean; fields: []; addition: []; size: number; commit: boolean; cancelToken: boolean }
     * @returns
     */
    requestIndexSetValueList({ commit, state }, payload) {
      const { start_time, end_time } = state.indexItem;
      const lastQueryTimerange = `${start_time}_${end_time}`;

      const cancelTokenKey = 'requestIndexSetValueListCancelToken';
      RequestPool.execCanceToken(cancelTokenKey);
      const requestCancelToken = payload.cancelToken ? RequestPool.getCancelToken(cancelTokenKey) : null;

      // 本次请求与上次请求时间范围不一致，重置缓存数据
      if (state.indexFieldInfo.last_eggs_request_token !== lastQueryTimerange) {
        set(state.indexFieldInfo, 'aggs_items', {});
      }

      if (!!payload.force) {
        (payload?.fields ?? []).forEach(field => {
          set(state.indexFieldInfo.aggs_items, field.field_name, []);
        });
      }

      const isDefaultQuery = !(payload?.fields?.length ?? false);
      const filterBuildIn = field => (isDefaultQuery ? !field.is_built_in : true);

      const filterFn = field =>
        !state.indexFieldInfo.aggs_items[field.field_name]?.length &&
        field.es_doc_values &&
        filterBuildIn(field) &&
        ['keyword', 'integer', 'long', 'double', 'bool', 'conflict'].includes(field.field_type) &&
        !/^__dist_/.test(field.field_name);

      const mapFn = field => field.field_name;
      const fields = (payload?.fields?.length ? payload.fields : state.indexFieldInfo.fields)
        .filter(filterFn)
        .map(mapFn);

      if (!fields.length) return Promise.resolve(true);

      const urlStr = state.indexItem.isUnionIndex ? 'unionSearch/unionTerms' : 'retrieve/getAggsTerms';
      const queryData = {
        keyword: '*',
        fields,
        addition: payload?.addition ?? [],
        start_time: formatDate(start_time),
        end_time: formatDate(end_time),
        size: payload?.size ?? 100,
      };

      if (state.indexItem.isUnionIndex) {
        Object.assign(queryData, {
          index_set_ids: state.unionIndexList,
        });
      }

      const params = {
        index_set_id: state.indexId,
      };

      const body = {
        params,
        data: queryData,
      };

      return http
        .request(urlStr, body, {
          cancelToken: requestCancelToken,
        })
        .then(resp => {
          if (payload?.commit !== false) {
            commit('updateIndexFieldEggsItems', resp.data.aggs_items ?? {});
          }

          return resp;
        });
    },

    requestFavoriteList({ commit, state }, payload) {
      commit('updateFavoriteList', []);
      return http
        .request('favorite/getFavoriteByGroupList', {
          query: {
            space_uid: payload?.spaceUid ?? state.spaceUid,
            order_type: payload?.sort ?? (localStorage.getItem('favoriteSortType') || 'NAME_ASC'),
          },
        })
        .then(resp => {
          commit('updateFavoriteList', resp.data || []);
          return resp;
        });
    },

    /**
     * 下钻添加条件到查询搜索
     * @param {*} param0
     * @param {*} payload
     * @returns
     */
    setQueryCondition({ state, dispatch }, payload) {
      const newQueryList = Array.isArray(payload) ? payload : [payload];
      const isLink = newQueryList[0]?.isLink;
      const searchMode = state.indexItem.search_mode;
      const depth = Number(payload.depth ?? '0');
      const isNestedField = payload?.isNestedField ?? 'false';
      const isNewSearchPage = newQueryList[0].operator === 'new-search-page-is';

      const getTargetField = field => {
        return state.visibleFields?.find(item => item.field_name === field);
      };

      const getFieldType = field => {        
        return getTargetField(field)?.field_type ?? '';
      };

      const getAdditionMappingOperator = ({ operator, field, value }) => {
        let mappingKey = {
          // is is not 值映射
          is: '=',
          'is not': '!=',
        };

        /** text类型字段类型的下钻映射 */
        const textMappingKey = {
          is: 'contains match phrase',
          'is not': 'not contains match phrase',
        };

        /** keyword 类型字段类型的下钻映射 */
        const keywordMappingKey = {
          is: 'contains',
          'is not': 'not contains',
        };

        const boolMapping = {
          is: `is ${value[0]}`,
          'is not': `is ${/true/i.test(value[0]) ? 'false' : 'true'}`,
        };

        const targetField = getTargetField(field);

        const textType = targetField?.field_type ?? '';
        const isVirtualObjNode = targetField?.is_virtual_obj_node ?? false;


        if (textType === 'text') {
          mappingKey = textMappingKey;
        }

        if (textType === 'boolean') {
          mappingKey = boolMapping;
          if (value.length) {
            value.splice(0, value.length);
          }
        }

        if ((depth > 1 || isNestedField === 'true') && textType === 'keyword') {
          mappingKey = keywordMappingKey;
        }
        return mappingKey[operator] ?? operator; // is is not 值映射
      };

      const formatJsonString = formatResult => {
        if (typeof formatResult === 'string') {
          return formatResult.replace(/"/g, '\\"');
        }

        return formatResult;
      };

      const getSqlAdditionMappingOperator = ({ operator, field }) => {
        const textType = getFieldType(field);

        const formatValue = value => {
          let formatResult = value;
          if (['text', 'string', 'keyword'].includes(textType)) {
            if (Array.isArray(formatResult)) {
              formatResult = formatResult.map(formatJsonString);
            } else {
              formatResult = formatJsonString(formatResult);
            }
          }

          return formatResult;
        };

        let mappingKey = {
          // is is not 值映射
          is: val => `${field}: "${formatValue(val)}"`,
          'is not': val => `NOT ${field}: "${formatValue(val)}"`,
        };

        return mappingKey[operator] ?? operator; // is is not 值映射
      };
      /** 判断条件是否已经在检索内 */
      const searchValueIsExist = (newSearchValue, searchMode) => {
        let isExist;
        if (searchMode === 'ui') {
          isExist = state.indexItem.addition.some(addition => {
            return (
              addition.field === newSearchValue.field &&
              addition.operator === newSearchValue.operator &&
              addition.value.toString() === newSearchValue.value.toString()
            );
          });
        }
        if (searchMode === 'sql') {
          const keyword = state.indexItem.keyword.replace(/^\s*\*\s*$/, '');
          isExist = keyword.indexOf(newSearchValue) !== -1;
        }
        return isExist;
      };
      const filterQueryList = newQueryList
        .map(item => {
          const isNewSearchPage = item.operator === 'new-search-page-is';
          item.operator = isNewSearchPage ? 'is' : item.operator;
          const { field, operator, value } = item;
          const targetField = getTargetField(field);


          let newSearchValue = null;
          if (searchMode === 'ui') {
            if (targetField?.is_virtual_obj_node) {
              newSearchValue = Object.assign({ field: '*', value }, { operator: 'contains match phrase' });
            } else {
              const mapOperator = getAdditionMappingOperator({ field, operator, value });
              newSearchValue = Object.assign({ field, value }, { operator: mapOperator });
            }
          }
          if (searchMode === 'sql') {
            if (targetField?.is_virtual_obj_node) { 
              newSearchValue = [value];
            } else{
              newSearchValue = getSqlAdditionMappingOperator({ field, operator })?.(value);
            }
          }
          const isExist = searchValueIsExist(newSearchValue, searchMode);
          return !isExist || isNewSearchPage ? newSearchValue : null;
        })
        .filter(Boolean);

      // list内的所有条件均相同时不进行添加条件处理
      if (!filterQueryList.length) return Promise.resolve([filterQueryList, searchMode, isNewSearchPage]);
      if (!isLink) {
        if (searchMode === 'ui') {
          const startIndex = state.indexItem.addition.length;
          state.indexItem.addition.splice(startIndex, 0, ...filterQueryList);
          dispatch('requestIndexSetQuery');
        }

        if (searchMode === 'sql') {
          const keyword = state.indexItem.keyword.replace(/^\s*\*\s*$/, '');
          const keywords = keyword.length > 0 ? [keyword] : [];
          const newSearchKeywords = filterQueryList.filter(item => keyword.indexOf(item) === -1);
          if (newSearchKeywords.length) {
            const lastIndex = newSearchKeywords.length - 1;
            newSearchKeywords[lastIndex] = newSearchKeywords[lastIndex].replace(/\s*$/, ' ');
          }

          const newSearchKeyword = keywords.concat(newSearchKeywords).join('AND ');
          state.indexItem.keyword = newSearchKeyword;
          dispatch('requestIndexSetQuery');
        }
      }
      return Promise.resolve([filterQueryList, searchMode, isNewSearchPage]);
    },

    changeShowUnionSource({ commit, dispatch, state }) {
      commit('updateIndexSetOperatorConfig', { isShowSourceField: !state.indexSetOperatorConfig.isShowSourceField });
      dispatch('showShowUnionSource', { keepLastTime: false });
    },

    /** 日志来源显隐操作 */
    showShowUnionSource({ state }, { keepLastTime = false }) {
      // 非联合查询 或者清空了所有字段 不走逻辑
      if (!state.indexItem.isUnionIndex || !state.visibleFields.length) return;
      const isExist = state.visibleFields.some(item => item.tag === 'union-source');
      // 保持之前的逻辑
      if (keepLastTime) {
        const isShowSourceField = state.indexSetOperatorConfig.isShowSourceField;
        if (isExist) {
          !isShowSourceField && state.visibleFields.shift();
        } else {
          isShowSourceField && state.visibleFields.unshift(logSourceField());
        }
        return;
      }

      isExist ? state.visibleFields.shift() : state.visibleFields.unshift(logSourceField());
    },
    requestSearchTotal({ state, getters }) {
      state.searchTotal = 0;
      const start_time = Math.floor(getters.retrieveParams.start_time);
      const end_time = Math.ceil(getters.retrieveParams.end_time);
      http
        .request(
          'retrieve/fieldStatisticsTotal',
          {
            data: {
              ...getters.retrieveParams,
              bk_biz_id: state.bkBizId,
              index_set_ids: state.indexItem.ids,
              start_time,
              end_time,
              addition: [...getters.retrieveParams.addition, ...(getters.common_filter_addition ?? [])],
            },
          },
          {
            catchIsShowMessage: false,
          },
        )
        .then(res => {
          const { data, code } = res;
          if (code === 0) state.searchTotal = data.total_count;
        });
    },
    setApiError({ commit }, payload) {
      commit('SET_API_ERROR', payload);
    },
    clearApiError({ commit }, apiName) {
      commit('CLEAR_API_ERROR', apiName);
    },

    handleTrendDataZoom({ commit, getters }, payload) {
      const { start_time, end_time, format } = payload;
      const formatStr = getters.retrieveParams.format;

      const [startTimeStamp, endTimeStamp] = format
        ? handleTransformToTimestamp([start_time, end_time], formatStr)
        : [start_time, end_time];

      commit('updateIndexItem', {
        start_time: startTimeStamp,
        end_time: endTimeStamp,
        datePickerValue: [start_time, end_time],
      });

      // 这里通过增加 prefix 标识当前是由图表缩放导致的更新操作
      // 用于后续逻辑判定使用
      commit('retrieve/updateChartKey', { prefix: 'chart_zoom_' });
    },
    userFieldConfigChange({ state, getters, commit }, userConfig) {
      return new Promise(async (resolve, reject) => {
        const indexSetConfig = {
          ...state.retrieve.catchFieldCustomConfig,
          ...userConfig,
        };
        const queryParams = {
          index_set_id: state.indexId,
          index_set_type: getters.isUnionSearch ? 'union' : 'single',
          index_set_config: indexSetConfig,
        };
        if (getters.isUnionSearch) {
          delete queryParams.index_set_id;
          queryParams.index_set_ids = state.unionIndexList;
        }
        try {
          const res = await http.request('retrieve/updateUserFiledTableConfig', {
            data: queryParams,
          });
          if (res.code === 0) {
            const userConfig = res.data.index_set_config;
            commit('retrieve/updateCatchFieldCustomConfig', userConfig);
          }
          resolve(res);
        } catch (err) {
          reject(err);
        }
      });
    },
  },
});

/**
 * hack vuex dispatch, add third parameter `config` to the dispatch method
 *
 * 需要对单独的请求做配置的话，无论是 get 还是 post，store.dispatch 都需要三个参数，例如：
 * store.dispatch('example/btn1', {btn: 'btn1'}, {fromCache: true})
 * 其中第二个参数指的是请求本身的参数，第三个参数指的是请求的配置，如果请求本身没有参数，那么
 * 第二个参数也必须占位，store.dispatch('example/btn1', {}, {fromCache: true})
 * 在 store 中需要如下写法：
 * btn1 ({ commit, state, dispatch }, params, config) {
 *     return http.get(`/app/index?invoke=btn1`, params, config)
 * }
 *
 * @param {Object|string} _type vuex type
 * @param {Object} _payload vuex payload
 * @param {Object} config config 参数，主要指 http 的参数，详见 src/api/index initConfig
 *
 * @return {Promise} 执行请求的 promise
 */
store.dispatch = function (_type, _payload, config = {}) {
  const { type, payload } = unifyObjectStyle(_type, _payload);

  const action = { type, payload, config };
  const entry = store._actions[type];
  if (!entry) {
    if (process.env.NODE_ENV !== 'production') {
      console.error(`[vuex] unknown action type: ${type}`);
    }
    return;
  }

  store._actionSubscribers
    .slice()
    .filter(sub => sub.before)
    .forEach(sub => sub.before(action, store.state));
  // store._actionSubscribers.forEach(sub => sub(action, store.state));

  return entry.length > 1 ? Promise.all(entry.map(handler => handler(payload, config))) : entry[0](payload, config);
};

export default store;
