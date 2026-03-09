#!/usr/bin/env python3
"""start_vm_runbook_v2.py

Azure VM起動Runbook
- VMを起動
- VMの起動状態を確認（タイムアウトあり）
- 起動後待機
- 各アラートルールの条件を評価（発報の恐れがないか確認・最大リトライあり）
- アラートルールを有効化
- アラートルールの有効化を確認

使い方:
    python start_vm_runbook_v2.py

設定:
    VM_START_CONFIG: VMとアラートルールのマッピング（タイムアウト・待機時間を含む）
    RETRY_CONFIG: API操作のリトライ設定
    ALERT_CONDITION_RETRY_CONFIG: アラート条件評価のリトライ設定
"""

from __future__ import annotations

import logging
import sys
import time
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Tuple
from functools import wraps

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.resourcehealth import ResourceHealthMgmtClient
from azure.monitor.query import LogsQueryClient, LogsQueryResult, LogsQueryStatus

# ============================================================================
# 設定セクション
# ============================================================================

# タイムゾーン
JST = timezone(timedelta(hours=9))

# VM起動対象とアラートルールの定義
# キー: VMのリソースID
# 値:
#   alert_rules                  : アラートルールのリソースIDのリスト
#   log_analytics_workspace_id   : Log AnalyticsワークスペースのカスタマーID（GUID）
#                                  Azure Portal > Log Analytics ワークスペース > 概要 > ワークスペースID
#                                  ※ scheduledqueryrules / activitylogalerts の条件評価に使用
#   vm_start_timeout_seconds     : VM起動確認タイムアウト（秒）
#   post_start_wait_seconds      : 起動後・アラート条件評価前の待機時間（秒）
VM_START_CONFIG: Dict[str, Dict[str, Any]] = {
    "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.compute/virtualMachines/your-vm-name": {
        "alert_rules": [
        "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/scheduledqueryrules/your-alert-rule-1",
        "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/activityLogAlerts/your-alert-rule-2",
        "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/metricalerts/your-alert-rule-3",
    ],
        "log_analytics_workspace_id": "00000000-0000-0000-0000-000000000000",  # Log AnalyticsワークスペースのカスタマーID（GUID）
        "post_start_wait_seconds": 120,    # 起動後待機時間（秒）
        "vm_start_timeout_seconds": 120,   # VM起動確認タイムアウト（秒）
    },
    # 追加のVMがあればここに記載
    # "/subscriptions/xxx/.../virtualMachines/vm-2": { ... },
}

# API操作用リトライ設定
RETRY_CONFIG: Dict[str, Any] = {
    "max_retries": 3,    # 最大リトライ回数
    "retry_delay": 10,   # リトライ間隔（秒）
}

# アラート条件評価用リトライ設定
ALERT_CONDITION_RETRY_CONFIG: Dict[str, Any] = {
    "max_retries": 3,             # 最大リトライ回数
    "retry_interval_seconds": 60, # リトライ間隔（秒）
}

# AzureActivityテーブルのフィールドマッピング（activitylogalerts条件評価で使用）
# アラートルールのcondition.all_of[n].field → AzureActivityテーブルの列名
ACTIVITY_LOG_FIELD_MAP: Dict[str, str] = {
    "category":        "CategoryValue",
    "status":          "ActivityStatusValue",
    "substatus":       "ActivitySubstatusValue",
    "operationname":   "OperationNameValue",
    "resourcetype":    "ResourceTypeValue",
    "resourcegroup":   "ResourceGroup",
    "level":           "Level",
    "caller":          "Caller",
    "properties.cause":                   "Properties_d.cause",
    "properties.currenthealthstatus":     "Properties_d.currentHealthStatus",
    "properties.previoushealthstatus":    "Properties_d.previousHealthStatus",
}

# アラートタイプ → MonitorManagementClient の属性名マッピング
_ALERT_TYPE_MAP: Dict[str, str] = {
    "microsoft.insights/metricalerts":        "metric_alerts",
    "microsoft.insights/scheduledqueryrules": "scheduled_query_rules",
    "microsoft.insights/activitylogalerts":   "activity_log_alerts",
}



# ============================================================================
# ログ設定
# ============================================================================

def setup_logging() -> logging.Logger:
    logging.getLogger("azure").setLevel(logging.WARNING)

    formatter = logging.Formatter(fmt="%(levelname)s: %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger


# ============================================================================
# ユーティリティ関数
# ============================================================================

def parse_resource_id(resource_id: str) -> Dict[str, str]:
    """
    Azure リソースIDをパースしてコンポーネントを抽出

    例: /subscriptions/{sub}/resourceGroups/{rg}/providers/{provider}/{type}/{name}
    """
    pattern = (
        r"/subscriptions/(?P<subscription>[^/]+)"
        r"/resourceGroups/(?P<resource_group>[^/]+)"
        r"/providers/(?P<provider>[^/]+)"
        r"/(?P<resource_type>[^/]+)"
        r"/(?P<resource_name>[^/]+)"
    )
    match = re.match(pattern, resource_id, re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid Azure resource ID format: {resource_id}")
    return match.groupdict()


def timedelta_to_iso8601(td: timedelta) -> str:
    """
    timedelta を ISO8601 期間文字列に変換

    例: timedelta(minutes=5) → "PT5M"
    """
    total_seconds = int(td.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    result = "P"
    if days:
        result += f"{days}D"
    result += "T"
    if hours:
        result += f"{hours}H"
    if minutes:
        result += f"{minutes}M"
    if seconds:
        result += f"{seconds}S"
    if result == "PT":
        result = "PT0S"
    return result


def compare_with_operator(value: float, operator: str, threshold: float) -> bool:
    """
    値と閾値をオペレータで比較し、アラートが発報される条件を満たすか判定

    Returns:
        True  = アラートが発報される条件を満たす（危険）
        False = アラートが発報される条件を満たさない（安全）
    """
    op = operator.lower().replace(" ", "")
    if op in ("greaterthan", ">"):
        return value > threshold
    elif op in ("greaterthanorequal", ">="):
        return value >= threshold
    elif op in ("lessthan", "<"):
        return value < threshold
    elif op in ("lessthanorequal", "<="):
        return value <= threshold
    elif op in ("equalto", "equal", "=", "=="):
        return value == threshold
    elif op in ("notequalto", "notequal", "!="):
        return value != threshold
    else:
        raise ValueError(f"サポートされていないオペレータです: {operator}")


def retry(logger: logging.Logger, config: Dict[str, Any]):
    """リトライデコレータ（固定間隔）"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            max_retries = config.get("max_retries", 3)
            delay = config.get("retry_delay", 10)
            for attempt in range(1, max_retries + 2):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt > max_retries:
                        logger.error(f"{func.__name__} が {max_retries} 回のリトライ後に失敗: {e}")
                        raise
                    logger.warning(f"{func.__name__} 失敗 (試行 {attempt}/{max_retries}): {e}")
                    time.sleep(delay)
        return wrapper
    return decorator


# ============================================================================
# Azure操作クラス
# ============================================================================

class AzureStartRunbookOperations:
    """Azure VM起動Runbook操作クラス"""

    def __init__(
        self,
        credential: DefaultAzureCredential,
        logger: logging.Logger,
        retry_config: Dict[str, Any],
        alert_condition_retry_config: Dict[str, Any],
    ):
        self.credential = credential
        self.logger = logger
        self.retry_config = retry_config
        self.alert_condition_retry_config = alert_condition_retry_config
        self._compute_clients: Dict[str, ComputeManagementClient] = {}
        self._monitor_clients: Dict[str, MonitorManagementClient] = {}
        self._resourcehealth_clients: Dict[str, ResourceHealthMgmtClient] = {}
        self._logs_query_client: Optional[LogsQueryClient] = None

    # ------------------------------------------------------------------
    # クライアントキャッシュ
    # ------------------------------------------------------------------

    def _compute(self, subscription_id: str) -> ComputeManagementClient:
        """ComputeManagementClientをサブスクリプション単位でキャッシュして返す"""
        if subscription_id not in self._compute_clients:
            self._compute_clients[subscription_id] = ComputeManagementClient(self.credential, subscription_id)
        return self._compute_clients[subscription_id]

    def _monitor(self, subscription_id: str) -> MonitorManagementClient:
        """MonitorManagementClientをサブスクリプション単位でキャッシュして返す"""
        if subscription_id not in self._monitor_clients:
            self._monitor_clients[subscription_id] = MonitorManagementClient(self.credential, subscription_id)
        return self._monitor_clients[subscription_id]

    def _resourcehealth(self, subscription_id: str) -> ResourceHealthMgmtClient:
        """ResourceHealthMgmtClientをサブスクリプション単位でキャッシュして返す"""
        if subscription_id not in self._resourcehealth_clients:
            self._resourcehealth_clients[subscription_id] = ResourceHealthMgmtClient(self.credential, subscription_id)
        return self._resourcehealth_clients[subscription_id]

    def _logs_client(self) -> LogsQueryClient:
        """LogsQueryClientをシングルトンとして返す"""
        if self._logs_query_client is None:
            self._logs_query_client = LogsQueryClient(credential=self.credential)
        return self._logs_query_client

    def _alert_ops(self, alert_resource_id: str):
        """
        アラートリソースIDからMonitorクライアントの操作オブジェクトとパース結果を返す

        Returns:
            tuple: (操作オブジェクト, parse_resource_id の返り値)
        """
        p = parse_resource_id(alert_resource_id)
        full_type = f"{p['provider']}/{p['resource_type']}".lower()
        attr = _ALERT_TYPE_MAP.get(full_type)
        if attr is None:
            raise ValueError(f"サポートされていないアラートルールの種類です: {full_type}")
        return getattr(self._monitor(p["subscription"]), attr), p

    # ------------------------------------------------------------------
    # ステップ1: VM起動
    # ------------------------------------------------------------------

    def start_vm(self, vm_resource_id: str) -> None:
        """VMを起動"""
        p = parse_resource_id(vm_resource_id)
        rg, vm_name = p["resource_group"], p["resource_name"]

        @retry(self.logger, self.retry_config)
        def _start():
            poller = self._compute(p["subscription"]).virtual_machines.begin_start(rg, vm_name)
            poller.result()

        _start()

    # ------------------------------------------------------------------
    # ステップ2: VM起動状態確認
    # ------------------------------------------------------------------

    def wait_for_vm_running(self, vm_resource_id: str, timeout_seconds: int, poll_interval_seconds: int = 15) -> bool:
        """
        VMがrunning状態になるまで待機（タイムアウトあり）

        Args:
            vm_resource_id: VMのリソースID
            timeout_seconds: タイムアウト秒数
            poll_interval_seconds: ポーリング間隔（秒）

        Returns:
            bool: タイムアウト前にrunningになればTrue
        """
        p = parse_resource_id(vm_resource_id)
        rg, vm_name = p["resource_group"], p["resource_name"]

        client = self._compute(p["subscription"])

        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)
        attempt = 0

        while datetime.now(timezone.utc) < deadline:
            attempt += 1
            try:
                iv = client.virtual_machines.instance_view(rg, vm_name)
                provisioning_state = next(
                    (s.code for s in (iv.statuses or []) if s.code and s.code.startswith("ProvisioningState/")),
                    None
                )
                power_state = next(
                    (s.code for s in (iv.statuses or []) if s.code and s.code.startswith("PowerState/")),
                    None
                )
                print("    - プロビジョニング: {}".format(provisioning_state or '不明'))
                print("    - 電源: {}".format(power_state or '不明'))
                if provisioning_state == "ProvisioningState/succeeded" and power_state == "PowerState/running":
                    print(f"  -> VMの起動を確認")
                    return True
            except Exception as e:
                self.logger.warning(f"  VMステータス取得中にエラー発生 (試行 {attempt}): {e}")

            remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
            if remaining > poll_interval_seconds:
                time.sleep(poll_interval_seconds)
            else:
                break
        print(f"  -> タイムアウト: VMが {timeout_seconds} 秒以内に起動状態になりませんでした")
        return False

    # ------------------------------------------------------------------
    # ステップ4: アラート条件評価
    # ------------------------------------------------------------------

    def evaluate_metric_alert_condition(
        self,
        alert_resource_id: str,
        vm_resource_id: str,
    ) -> Tuple[bool, str]:
        """
        メトリックアラートの条件を評価し、発報の恐れがないか確認

        Returns:
            (is_safe, detail_message)
            is_safe = True  → 安全（発報の恐れなし）
            is_safe = False → 条件達成（発報の恐れあり）
        """
        p = parse_resource_id(alert_resource_id)
        monitor_client = self._monitor(p["subscription"])
        alert = monitor_client.metric_alerts.get(p["resource_group"], p["resource_name"])
        alert_name = p["resource_name"]

        all_of = getattr(alert.criteria, "all_of", None)
        if not all_of:
            self.logger.warning(f"メトリックアラートに評価可能な条件がありません: {alert_name}")
            return True, "条件なし（スキップ）"

        window_size_td: timedelta = alert.window_size  # SDK は timedelta で返す
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - window_size_td
        timespan = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}/{now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"

        for criterion in all_of:
            criterion_type = getattr(criterion, "criterion_type", "") or ""

            # DynamicThresholdCriterion は具体的な判定基準がないため評価不可 → 発報を許容しスキップして安全側に倒す
            if "dynamic" in criterion_type.lower():
                self.logger.warning(
                    f"  DynamicThresholdCriterion は評価基準がないためスキップします: "
                    f"{alert_name} / {getattr(criterion, 'metric_name', '不明')}"
                )
                continue

            metric_name: str = criterion.metric_name
            operator: str = str(criterion.operator)
            threshold: float = float(criterion.threshold)
            time_aggregation: str = str(criterion.time_aggregation)

            print(f"      - 種別: メトリックアラート [{metric_name}]")
            print(f"      - 集計: {time_aggregation}")
            print(f"      - 条件: {operator} {threshold}")
            print(f"      - 期間: {alert.window_size}")

            metrics_data = monitor_client.metrics.list(
                resource_uri=vm_resource_id,
                timespan=timespan,
                interval=timedelta_to_iso8601(window_size_td),
                metricnames=metric_name,
                aggregation=time_aggregation,
            )

            metric_value: Optional[float] = None
            if metrics_data.value:
                ts_list = metrics_data.value[0].timeseries
                if ts_list and ts_list[0].data:
                    latest = ts_list[0].data[-1]
                    agg_lower = time_aggregation.lower()
                    metric_value = getattr(latest, agg_lower, None)  # average / total / minimum / maximum / count

            # メトリック値が取得できなかった場合は危険判定
            if metric_value is None:
                self.logger.warning(
                    f"  メトリック値を取得できませんでした: {alert_name} / {metric_name}。"
                )
                detail = "データポイントが存在しない可能性があります。"
                return False, detail
            
            print(f"      - 現在の値: {metric_value}")

            if compare_with_operator(metric_value, operator, threshold):
                detail = f"{metric_name}={metric_value:.4g} が条件 {operator} {threshold} を満たしています"
                print(f"    -> アラート発報の可能性があります:")
                print(f"       詳細: {detail}")
                return False, detail

        print("      -> OK")
        return True, "安全"

    def evaluate_scheduled_query_rule_condition(
        self,
        alert_resource_id: str,
        log_analytics_workspace_id: str,
    ) -> Tuple[bool, str]:
        """
        ログアラートルール（Scheduled Query Rule）の条件を評価し、発報の恐れがないか確認

        Returns:
            (is_safe, detail_message)
        """
        p = parse_resource_id(alert_resource_id)
        monitor_client = self._monitor(p["subscription"])
        rule = monitor_client.scheduled_query_rules.get(p["resource_group"], p["resource_name"])
        rule_name = p["resource_name"]

        if not rule.criteria or not rule.criteria.all_of:
            self.logger.warning(f"スケジュールクエリルールに評価可能な条件がありません: {rule_name}")
            return True, "条件なし（スキップ）"

        if rule.window_size is None:
            raise ValueError(f"スケジュールクエリルールのwindow_sizeが未設定です: {rule_name}")
        window_size_td: timedelta = rule.window_size
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - window_size_td
        logs_client = self._logs_client()

        for idx, criterion in enumerate(rule.criteria.all_of, 1):
            if criterion.query is None:
                self.logger.warning(f"クエリが未設定のため条件をスキップします [{idx}]: {rule_name}")
                continue
            if criterion.threshold is None:
                self.logger.warning(f"閾値が未設定のため条件をスキップします [{idx}]: {rule_name}")
                continue

            query: str = criterion.query
            operator: str = str(criterion.operator)
            threshold: float = float(criterion.threshold)
            time_aggregation: str = str(criterion.time_aggregation)

            print(f"      - 種別: ログアラート")
            print(f"      - 集計: {time_aggregation}")
            print(f"      - 条件: {operator} {threshold}")
            print(f"      - 期間: {rule.window_size}")
            self.logger.debug(f"  クエリ: {query}")

            response = logs_client.query_workspace(
                workspace_id=log_analytics_workspace_id,
                query=query,
                timespan=(start_utc, now_utc),
            )

            if response.status == LogsQueryStatus.FAILURE:
                raise RuntimeError(
                    f"Log Analyticsクエリが失敗しました: {rule_name} "
                    f"(エラー: {getattr(response, 'partial_error', 'Unknown')})"
                )
            if response.status == LogsQueryStatus.PARTIAL:
                self.logger.warning(f"Log Analyticsクエリが部分的な結果を返しました: {rule_name}")

            _tables = response.tables if isinstance(response, LogsQueryResult) else (response.partial_data or [])
            metric_value: float = 0.0
            if _tables and _tables[0].rows:
                rows = _tables[0].rows
                agg_lower = time_aggregation.lower()
                if agg_lower == "count":
                    metric_value = float(len(rows))
                else:
                    measure_col: Optional[str] = getattr(criterion, "metric_measure_column", None)
                    if measure_col and _tables[0].columns:
                        col_names = list(_tables[0].columns)
                        if measure_col in col_names:
                            col_idx = col_names.index(measure_col)
                            values = [float(row[col_idx]) for row in rows if row[col_idx] is not None]
                            if values:
                                if agg_lower == "average":
                                    metric_value = sum(values) / len(values)
                                elif agg_lower == "total":
                                    metric_value = sum(values)
                                elif agg_lower == "minimum":
                                    metric_value = min(values)
                                elif agg_lower == "maximum":
                                    metric_value = max(values)
                            else:
                                metric_value = 0.0
                        else:
                            self.logger.warning(
                                f"metric_measure_column '{measure_col}' が結果に見つかりません。行数でカウントします。"
                            )
                            metric_value = float(len(rows))
                    else:
                        metric_value = float(len(rows))
            print(f"      - クエリ結果の集計値: {metric_value}")

            if compare_with_operator(metric_value, operator, threshold):
                detail = f"クエリ集計値={metric_value} が条件 {operator} {threshold} を満たしています"
                print(f"    -> アラート発報の可能性があります:")
                print(f"       詳細: {detail}")
                return False, detail

        print("      -> OK")
        return True, "安全"

    def evaluate_activity_log_alert_condition(
        self,
        alert_resource_id: str,
        log_analytics_workspace_id: str,
        vm_start_time: datetime,
    ) -> Tuple[bool, str]:
        """
        アクティビティログアラートの条件を評価し、発報の恐れがないか確認

        カテゴリに応じて評価方法を切り替える:
        - ResourceHealth : 最新ヘルスイベントがアラート条件に一致するか確認
        - その他          : VM起動後の条件一致イベント件数を確認

        Returns:
            (is_safe, detail_message)
        """
        p = parse_resource_id(alert_resource_id)
        monitor_client = self._monitor(p["subscription"])
        alert = monitor_client.activity_log_alerts.get(p["resource_group"], p["resource_name"])
        alert_name = p["resource_name"]

        if not alert.condition or not alert.condition.all_of:
            self.logger.warning(f"アクティビティログアラートに評価可能な条件がありません: {alert_name}")
            return True, "条件なし（スキップ）"

        # カテゴリを条件から抽出
        category: Optional[str] = None
        for cond in alert.condition.all_of:
            if cond.field and cond.field.lower() == "category" and cond.equals:
                category = cond.equals.lower()
                break

        # スコープ（監視対象リソースID）を取得
        scopes: list[str] = list(alert.scopes or [])

        now_utc = datetime.now(timezone.utc)

        print(f"      - 種別: アクティビティログアラート (カテゴリ: {category or '不明'})")

        if category == "resourcehealth":
            return self._evaluate_resourcehealth_alert_condition(
                alert_name, scopes, alert.condition.all_of, p["subscription"],
            )
        else:
            return self._evaluate_activity_log_alert_count_condition(
                alert_name, log_analytics_workspace_id, scopes,
                alert.condition.all_of, vm_start_time, now_utc,
            )

    def _evaluate_resourcehealth_alert_condition(
        self,
        alert_name: str,
        scopes: list[str],
        all_of_conditions,
        subscription_id: str,
    ) -> Tuple[bool, str]:
        """
        ResourceHealthカテゴリのアクティビティログアラート評価

        azure-mgmt-resourcehealth SDK を使用して現在のリソース正常性を直接取得し、
        アラート条件（currentHealthStatus / cause）に一致するか確認する。
        現在の状態が条件に一致しない（= 現在は正常状態）であれば安全と判断する。
        """
        rh_client = self._resourcehealth(subscription_id)

        # 評価対象リソース: scopes が設定されていればそれを使用、なければ評価不可
        if not scopes:
            self.logger.warning(f"アラートルールにscopesが設定されていないためスキップします: {alert_name}")
            return True, "scopes未設定（スキップ）"

        # アラート条件から評価対象フィールドを抽出
        # currentHealthStatus / cause のみ SDK で評価可能
        condition_currenthealth: Optional[str] = None
        condition_cause: Optional[str] = None
        for cond in all_of_conditions:
            if cond.field is None or cond.equals is None:
                continue
            field = cond.field.lower()
            if field == "properties.currenthealthstatus":
                condition_currenthealth = cond.equals
            elif field == "properties.cause":
                condition_cause = cond.equals
            elif field in ("category", "resourcetype"):
                pass  # 絞り込み条件のためスキップ
            else:
                self.logger.warning(
                    f"フィールド '{cond.field}' はResourceHealth APIでは評価できないためスキップします: {alert_name}"
                )

        for resource_id in scopes:
            try:
                status = rh_client.availability_statuses.get_by_resource(
                    resource_uri=resource_id,
                )
            except Exception as e:
                raise RuntimeError(
                    f"ResourceHealth APIの呼び出しに失敗しました: {alert_name} / {resource_id} ({e})"
                ) from e

            props = status.properties
            current_state: str = (props.availability_state or "") if props else ""
            reason_type: str = (props.reason_type or "") if props else ""
            occurred_time = props.occured_time if props else None
            summary: str = (props.summary or "") if props else ""

            resource_short = resource_id.split("/")[-1]
            print(f"      - リソース正常性 ({resource_short}):")
            print(f"          availabilityState : {current_state or '(なし)'}")
            print(f"          reasonType        : {reason_type or '(なし)'}")
            print(f"          occurredTime      : {occurred_time or '(なし)'}")
            print(f"          summary           : {summary or '(なし)'}")

            # currentHealthStatus 条件の照合
            if condition_currenthealth is not None:
                if current_state.lower() == condition_currenthealth.lower():
                    detail = (
                        f"現在のリソース正常性が '{current_state}' です"
                        f"（アラート条件: currentHealthStatus = {condition_currenthealth}）"
                    )
                    print(f"    -> アラート発報の可能性があります:")
                    print(f"       詳細: {detail}")
                    return False, detail

            # cause 条件の照合
            if condition_cause is not None:
                if reason_type.lower() == condition_cause.lower():
                    detail = (
                        f"リソース正常性の原因が '{reason_type}' です"
                        f"（アラート条件: cause = {condition_cause}）"
                    )
                    print(f"    -> アラート発報の可能性があります:")
                    print(f"       詳細: {detail}")
                    return False, detail

        print("      -> OK")
        return True, "安全"

    def _evaluate_activity_log_alert_count_condition(
        self,
        alert_name: str,
        log_analytics_workspace_id: str,
        scopes: list[str],
        all_of_conditions,
        vm_start_time: datetime,
        now_utc: datetime,
    ) -> Tuple[bool, str]:
        """
        ResourceHealth以外のアクティビティログアラート評価（VM起動後イベントカウント方式）

        VM起動時刻以降に条件一致イベントが0件であれば安全と判断する。
        """
        where_clauses: list[str] = []
        for cond in all_of_conditions:
            if cond.field is None or cond.equals is None:
                self.logger.debug(f"field/equals が未設定の条件をスキップ: {alert_name}")
                continue
            field = cond.field.lower()
            value = cond.equals
            kql_column = ACTIVITY_LOG_FIELD_MAP.get(field)
            if kql_column:
                if "." in kql_column:
                    where_clauses.append(f'tostring({kql_column}) =~ "{value}"')
                else:
                    where_clauses.append(f'{kql_column} =~ "{value}"')
            else:
                self.logger.warning(
                    f"フィールド '{cond.field}' のAzureActivityマッピングが未定義です。スキップします。"
                )

        # 有効なフィルタ条件が構築できない場合は評価不可のためスキップ（安全側に倒す）
        if not where_clauses:
            self.logger.warning(f"有効なフィルタ条件が構築できなかったためスキップします: {alert_name}")
            return True, "条件構築不可（スキップ）"

        start_str = vm_start_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        kql_lines = [
            "AzureActivity",
            f'| where TimeGenerated >= datetime("{start_str}")',
            f'| where TimeGenerated <= datetime("{end_str}")',
        ]
        # スコープフィルタ（監視対象リソースIDに限定し、他リソースのイベントを除外）
        if scopes:
            scope_values = ", ".join(f'"{s}"' for s in scopes)
            kql_lines.append(f"| where ResourceId in~ ({scope_values})")
        for clause in where_clauses:
            kql_lines.append(f"| where {clause}")
        kql_lines.append("| count")
        kql = "\n".join(kql_lines)

        self.logger.debug(f"  KQL クエリ:\n{kql}")

        response = self._logs_client().query_workspace(
            workspace_id=log_analytics_workspace_id,
            query=kql,
            timespan=(vm_start_time.astimezone(timezone.utc), now_utc),
        )

        if response.status == LogsQueryStatus.FAILURE:
            raise RuntimeError(
                f"Log AnalyticsクエリがAzureActivity照会で失敗しました: {alert_name} "
                f"(エラー: {getattr(response, 'partial_error', 'Unknown')})"
            )
        if response.status == LogsQueryStatus.PARTIAL:
            self.logger.warning(f"Log Analyticsクエリが部分的な結果を返しました: {alert_name}")

        _tables = response.tables if isinstance(response, LogsQueryResult) else (response.partial_data or [])
        count: int = int(_tables[0].rows[0][0]) if _tables and _tables[0].rows else 0

        print(f"      - VM起動後の一致イベント件数: {count}")

        if count > 0:
            detail = f"VM起動後に条件一致イベントが {count} 件見つかりました（発報の恐れあり）"
            print(f"    -> アラート発報の可能性があります:")
            print(f"       詳細: {detail}")
            return False, detail

        print("      -> OK")
        return True, "安全"

    def evaluate_alert_condition(
        self,
        alert_resource_id: str,
        vm_resource_id: str,
        log_analytics_workspace_id: str,
        vm_start_time: datetime,
    ) -> Tuple[bool, str]:
        """
        アラートルールの種類を自動判別して条件を評価

        Returns:
            (is_safe, detail_message)
        """
        p = parse_resource_id(alert_resource_id)
        full_type = f"{p['provider']}/{p['resource_type']}".lower()

        if full_type == "microsoft.insights/metricalerts":
            return self.evaluate_metric_alert_condition(alert_resource_id, vm_resource_id)
        elif full_type == "microsoft.insights/scheduledqueryrules":
            return self.evaluate_scheduled_query_rule_condition(alert_resource_id, log_analytics_workspace_id)
        elif full_type == "microsoft.insights/activitylogalerts":
            return self.evaluate_activity_log_alert_condition(alert_resource_id, log_analytics_workspace_id, vm_start_time)
        else:
            raise ValueError(f"サポートされていないアラートルールの種類です: {full_type}")

    def evaluate_alert_condition_with_retry(
        self,
        alert_resource_id: str,
        vm_resource_id: str,
        log_analytics_workspace_id: str,
        vm_start_time: datetime,
    ) -> Tuple[bool, str, int]:
        """
        アラート条件評価をリトライ付きで実行

        Returns:
            (is_safe, final_detail_message, retry_count_used)
        """
        max_retries = self.alert_condition_retry_config.get("max_retries", 10)
        retry_interval = self.alert_condition_retry_config.get("retry_interval_seconds", 60)
        alert_name = parse_resource_id(alert_resource_id)["resource_name"]

        last_detail = ""
        for attempt in range(1, max_retries + 2):
            try:
                is_safe, detail = self.evaluate_alert_condition(
                    alert_resource_id, vm_resource_id, log_analytics_workspace_id, vm_start_time
                )
                last_detail = detail
                if is_safe:
                    if attempt > 1:
                        print("      -> リトライ終了（安全が確認されました。）")
                    return True, detail, attempt - 1
                else:
                    if attempt <= max_retries:
                        self.logger.warning(
                            f"  [{attempt}/{max_retries}回目] "
                            f"アラート発報条件が満たされています。"
                            f"{retry_interval} 秒後に再評価します: {alert_name}"
                        )
                        time.sleep(retry_interval)
                    else:
                        self.logger.warning(
                            f"  最大リトライ回数 ({max_retries} 回) に達しました。"
                            f"アラート条件が依然として満たされています: {alert_name}"
                        )
                        return False, detail, attempt - 1
            except Exception as e:
                last_detail = str(e)
                if attempt <= max_retries:
                    self.logger.warning(
                        f"  [{attempt}/{max_retries}回目] "
                        f"アラート条件評価中にエラー: {e}。"
                        f"{retry_interval} 秒後にリトライします。"
                    )
                    time.sleep(retry_interval)
                else:
                    self.logger.error(f"  最大リトライ回数 ({max_retries} 回) 後もエラーが継続: {e}")
                    return False, f"評価エラー: {e}", attempt - 1

        return False, last_detail, max_retries

    # ------------------------------------------------------------------
    # ステップ5: アラートルール有効化
    # ------------------------------------------------------------------

    def enable_alert_rule(self, alert_resource_id: str) -> None:
        """アラートルールを有効化（種類を自動判別）"""
        ops, p = self._alert_ops(alert_resource_id)
        name, rg = p["resource_name"], p["resource_group"]

        @retry(self.logger, self.retry_config)
        def _enable():
            resource = ops.get(rg, name)
            resource.enabled = True
            ops.create_or_update(rg, name, resource)

        _enable()

    # ------------------------------------------------------------------
    # ステップ6: アラートルール有効化確認
    # ------------------------------------------------------------------

    def verify_alert_rule_enabled(self, alert_resource_id: str) -> bool:
        """アラートルールが有効化されているか確認（種類を自動判別）"""
        ops, p = self._alert_ops(alert_resource_id)
        name, rg = p["resource_name"], p["resource_group"]

        resource = ops.get(rg, name)
        is_enabled = bool(resource.enabled)

        return is_enabled

# ============================================================================
# メイン処理
# ============================================================================

def _set_alert_enable_failed(enables: list, name: str, error: str) -> None:
    """alert_enable リストの該当エントリをfailedに更新するヘルパー"""
    for a in enables:
        if a["name"] == name:
            a["status"] = "failed"
            a["error"] = error
            break


def print_execution_summary(execution_results: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    実行結果サマリを出力

    Returns:
        bool: 全て成功した場合True、失敗があった場合False
    """
    print("")
    print("=" * 80)
    print("VM起動Runbook 実行結果サマリ")
    print("=" * 80)

    start_time = execution_results["start_time"]
    end_time = execution_results["end_time"]
    print(f"実行開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')} JST")
    print(f"実行終了: {end_time.strftime('%Y-%m-%d %H:%M:%S')} JST")
    print(f"総処理時間: {str(end_time - start_time).split('.')[0]}")
    print("")

    total_vms = len(execution_results["vms"])
    success_count = failed_count = 0
    total_alerts = safe_conditions = unsafe_conditions = enabled_alerts = failed_alerts = 0

    for idx, vm_result in enumerate(execution_results["vms"], 1):
        print("-" * 80)
        print(f"【{idx}/{total_vms}: {vm_result['vm_name']}】")
        print("-" * 80)

        print("  VM起動:")
        vm_start = vm_result["vm_start"]
        if vm_start["status"] == "success":
            print(f"    ✓ 起動完了 ({vm_start['power_state']})")
        elif vm_start["status"] == "skipped":
            print(f"    ⊘ スキップ ({vm_start['error']})")
        else:
            print(f"    ✗ 失敗 ({vm_start['error']})")
        print("")

        print("  アラート条件評価:")
        for cond in vm_result["alert_conditions"]:
            total_alerts += 1
            if cond["status"] == "safe":
                safe_conditions += 1
                retries_str = f", リトライ: {cond['retries']}回" if cond["retries"] > 0 else ""
                print(f"    ✓ {cond['name']:<40s}: 安全{retries_str}")
            elif cond["status"] == "skipped":
                print(f"    ⊘ {cond['name']:<40s}: スキップ ({cond['detail']})")
            else:
                unsafe_conditions += 1
                print(f"    ✗ {cond['name']:<40s}: {cond['detail']}")
        print("")

        print("  アラートルール有効化:")
        for alert in vm_result["alert_enable"]:
            if alert["status"] == "success":
                enabled_alerts += 1
                print(f"    ✓ {alert['name']:<40s}: 有効化完了")
            elif alert["status"] == "skipped":
                print(f"    ⊘ {alert['name']:<40s}: スキップ ({alert['error']})")
            else:
                failed_alerts += 1
                print(f"    ✗ {alert['name']:<40s}: 有効化失敗 ({alert['error']})")
        print("")

        overall = vm_result["overall_status"]
        vm_duration = vm_result["end_time"] - vm_result["start_time"]
        icon = "✓" if overall == "success" else "✗"
        text = "成功" if overall == "success" else "失敗"
        print(f"  処理結果: {icon} {text}")
        print(f"  処理時間: {str(vm_duration).split('.')[0]}")
        print("")

        if overall == "success":
            success_count += 1
        else:
            failed_count += 1

    success_vms = [vm["vm_name"] for vm in execution_results["vms"] if vm["overall_status"] == "success"]
    failed_vms  = [vm["vm_name"] for vm in execution_results["vms"] if vm["overall_status"] == "failed"]

    print("=" * 80)
    print("【全体サマリ】")
    print(f"  対象VM数       : {total_vms}台")
    print(f"  成功           : {success_count}台" + (f" ({', '.join(success_vms)})" if success_vms else ""))
    print(f"  失敗           : {failed_count}台" + (f" ({', '.join(failed_vms)})" if failed_vms else ""))
    print("")
    print(f"  総アラートルール数  : {total_alerts}件")
    print(f"  条件評価 安全  : {safe_conditions}件")
    print(f"  条件評価 危険  : {unsafe_conditions}件（有効化スキップ）")
    print(f"  有効化成功     : {enabled_alerts}件")
    print(f"  有効化失敗     : {failed_alerts}件")
    print("")
    print("  実行結果: " + ("✓ 全て成功" if failed_count == 0 else "✗ 一部または全て失敗"))
    print("=" * 80)

    return failed_count == 0


def main():
    """VM起動Runbookのメイン処理"""
    starttime = datetime.now(JST)

    logger = setup_logging()
    print("=" * 80)
    print(f"VM起動Runbook開始 {starttime.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    execution_results: Dict[str, Any] = {
        "start_time": starttime,
        "end_time": None,
        "vms": [],
    }

    try:
        # ------------------------------------------------------------------
        # ステップ1: Azure認証
        # ------------------------------------------------------------------
        print("認証開始")
        credential = DefaultAzureCredential()
        operations = AzureStartRunbookOperations(
            credential=credential,
            logger=logger,
            retry_config=RETRY_CONFIG,
            alert_condition_retry_config=ALERT_CONDITION_RETRY_CONFIG,
        )
        print("-> 認証成功")
        print("")
        # VM台数のカウント
        total_vms = len(VM_START_CONFIG)

        # ------------------------------------------------------------------
        # ステップ2: VM起動処理
        # ------------------------------------------------------------------
        print("VM起動・抑止解除処理開始")
        # VMごとに処理開始
        for idx, (vm_resource_id, vm_config) in enumerate(VM_START_CONFIG.items(), 1):
            vm_name = parse_resource_id(vm_resource_id)["resource_name"]
            alert_rules: list[str] = vm_config["alert_rules"]
            log_analytics_workspace_id: str = vm_config["log_analytics_workspace_id"]
            vm_start_timeout: int = vm_config["vm_start_timeout_seconds"]
            post_start_wait: int = vm_config["post_start_wait_seconds"]

            print("[{}/{}] {}".format(idx, len(VM_START_CONFIG.keys()), vm_name))

            vm_result: Dict[str, Any] = {
                "vm_name": vm_name,
                "start_time": datetime.now(JST),
                "end_time": None,
                "vm_start": {"status": "skipped", "power_state": None, "error": "未実行"},
                "alert_conditions": [],
                "alert_enable": [],
                "overall_status": "failed",
            }

            # print("-" * 80)
            # print("")
            # print("-" * 80)

            try:
                # VM起動時刻を記録（activitylogalerts評価で使用）
                # start_vm() 呼び出し前に記録することで、起動プロセス中に発生したイベントを捕捉漏れなく評価できる
                vm_actual_start_time = datetime.now(timezone.utc)

                # ステップ1: VM起動
                print("  - 起動要求送信")
                operations.start_vm(vm_resource_id)

                # ステップ2: VM起動状態確認
                print("  - 起動確認中")
                if not operations.wait_for_vm_running(vm_resource_id, timeout_seconds=vm_start_timeout):
                    vm_result["vm_start"]["status"] = "failed"
                    vm_result["vm_start"]["error"] = f"起動確認タイムアウト ({vm_start_timeout}秒)"
                    raise Exception(f"VM {vm_name} が {vm_start_timeout}秒以内に起動状態になりませんでした")

                # wait_for_vm_running が True を返した時点で PowerState/running 確定
                vm_result["vm_start"]["status"] = "success"
                vm_result["vm_start"]["power_state"] = "PowerState/running"
                vm_result["vm_start"]["error"] = None

                # ステップ3: 起動後待機（ラート条件評価前の安定化を待機）
                print(f"  - 起動後待機中 ({post_start_wait}秒)")
                time.sleep(post_start_wait)

                # ステップ4: アラート条件評価（リトライあり）
                print("  - アラートルールを有効化可能か評価（{} 件）".format(len(alert_rules)))
                all_conditions_safe = True

                for i, alert_id in enumerate(alert_rules, 1):
                    alert_name = parse_resource_id(alert_id)["resource_name"]
                    print("    - [{}/{}] {}".format(i, len(alert_rules), alert_name))
                    try:
                        is_safe, detail, retries_used = operations.evaluate_alert_condition_with_retry(
                            alert_resource_id=alert_id,
                            vm_resource_id=vm_resource_id,
                            log_analytics_workspace_id=log_analytics_workspace_id,
                            vm_start_time=vm_actual_start_time,
                        )
                        vm_result["alert_conditions"].append({
                            "name": alert_name,
                            "status": "safe" if is_safe else "unsafe",
                            "detail": detail,
                            "retries": retries_used,
                            "error": None if is_safe else detail,
                        })
                        if not is_safe:
                            all_conditions_safe = False
                            logger.warning(f"  アラート発報の可能性があるため有効化をスキップしました: {alert_name} (Reason: {detail})")
                    except Exception as e:
                        error_msg = str(e).split("\n")[0]
                        vm_result["alert_conditions"].append({
                            "name": alert_name,
                            "status": "error",
                            "detail": error_msg,
                            "retries": 0,
                            "error": error_msg,
                        })
                        all_conditions_safe = False
                        logger.error(f"  アラート条件評価中に例外が発生しました: {alert_name}")
                        logger.exception(e)

                # 1つでもアラート発報条件を満たすものがある、または評価に失敗していればアラートルールを有効化しない
                if not all_conditions_safe:
                    for alert_id in alert_rules:
                        a_name = parse_resource_id(alert_id)["resource_name"]
                        vm_result["alert_enable"].append({
                            "name": a_name,
                            "status": "skipped",
                            "error": "アラート条件評価が安全でないためスキップ",
                        })
                    raise Exception(f"{vm_name}: アラート条件が安全でないため、全アラートルールの有効化をスキップします")

                print("    -> 評価完了")

                # ステップ5: アラートルール有効化
                print("  - アラートルールを有効化（{} 件）".format(len(alert_rules)))
                all_enabled = True

                for i, alert_id in enumerate(alert_rules, 1):
                    alert_name = parse_resource_id(alert_id)["resource_name"]
                    print("    - [{}/{}] アラートルールを有効化中: {}".format(i, len(alert_rules), alert_name))
                    try:
                        operations.enable_alert_rule(alert_id)
                        vm_result["alert_enable"].append({"name": alert_name, "status": "success", "error": None})
                    except Exception as e:
                        error_msg = str(e).split("\n")[0]
                        vm_result["alert_enable"].append({"name": alert_name, "status": "failed", "error": error_msg})
                        all_enabled = False
                        logger.error(f"  アラートルールの有効化に失敗: {alert_name}")
                        logger.exception(e)

                if not all_enabled:
                    raise Exception(f"{vm_name}: 一部のアラートルールの有効化に失敗しました")

                # ステップ6: アラートルール有効化確認
                print(f"  - アラートルールの有効化を確認")
                all_verified = True

                for i, alert_id in enumerate(alert_rules, 1):
                    alert_name = parse_resource_id(alert_id)["resource_name"]
                    print("    - [{}/{}] アラートルール有効化確認中: {}".format(i, len(alert_rules), alert_name))
                    try:
                        if not operations.verify_alert_rule_enabled(alert_id):
                            all_verified = False
                            _set_alert_enable_failed(vm_result["alert_enable"], alert_name, "有効化確認に失敗")
                    except Exception as e:
                        all_verified = False
                        logger.error(f"  有効化確認中にエラー: {alert_name}")
                        logger.exception(e)
                        _set_alert_enable_failed(vm_result["alert_enable"], alert_name, "確認エラー")

                if not all_verified:
                    raise Exception(f"{vm_name}: 一部のアラートルールの有効化確認に失敗しました")

                vm_result["overall_status"] = "success"
                print(f"  -> 有効化が確認されました")
            except Exception as e:
                logger.error(f"{vm_name} の処理に失敗: {e}")
                logger.exception(e)
                vm_result["overall_status"] = "failed"
            finally:
                vm_result["end_time"] = datetime.now(JST)
                execution_results["vms"].append(vm_result)

        execution_results["end_time"] = datetime.now(JST)

        if not print_execution_summary(execution_results, logger):
            raise Exception("VM起動処理の異常を検知しました。")

    except Exception as e:
        logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
