#!/usr/bin/env python3
"""stop_vm_runbook.py

Azure VM停止Runbook
- VMを監視しているアラートルールを無効化
- アラート無効化を確認
- VMを停止

使い方:
    python stop_vm_runbook.py

設定:
    VM_ALERT_CONFIG: VMとアラートルールのマッピング
    RETRY_CONFIG: リトライ設定
"""

from __future__ import annotations

import logging
import sys
import time
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from functools import wraps

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.core.exceptions import AzureError


# ============================================================================
# 設定セクション
# ============================================================================

# タイムゾーン
JST = timezone(timedelta(hours=9))

# VM停止対象とアラートルールの定義
# キー: VMのリソースID
# 値: アラートルールのリソースIDのリスト
VM_ALERT_CONFIG = {
    # 例: VM 1台とそれに関連するアラートルール2件
    "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.compute/virtualMachines/your-vm-name": [
        "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/scheduledqueryrules/your-alert-rule-1",
        "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/activityLogAlerts/your-alert-rule-2",
    ],
    # 追加のVMがあればここに記載
    # "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.compute/virtualMachines/your-vm-name-2": [
    #     "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/metricalerts/your-alert-rule-3",
    # ],
}

# リトライ設定
RETRY_CONFIG = {
    "max_retries": 3,           # 最大リトライ回数
    "initial_delay": 10,        # 初回待機時間（秒）
    "backoff_multiplier": 2,    # 指数バックオフの倍率
    "max_delay": 60,            # 最大待機時間（秒）
}

# ============================================================================
# ログ設定（JST対応）
# ============================================================================

class JSTFormatter(logging.Formatter):
    """Formatter that renders times in JST regardless of system timezone."""

    def __init__(self, fmt: Optional[str] = None, datefmt: Optional[str] = None, tz=None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.tz = tz or JST

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def setup_stdout_logging(
    level: int = logging.INFO,
    fmt: Optional[str] = None,
    datefmt: Optional[str] = None,
    tz=None,
    logger_name: Optional[str] = None,
    force: bool = False,
) -> logging.Logger:
    """Set up logging to stdout with JST timestamps and return a logger."""
    if fmt is None:
        fmt = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
    tz = tz or JST
    formatter = JSTFormatter(fmt=fmt, datefmt=datefmt, tz=tz)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()

    if not force:
        for h in root.handlers:
            if isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout:
                root.setLevel(level)
                return logging.getLogger(logger_name) if logger_name else root

    if force:
        root.handlers = [h for h in root.handlers if not (isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout)]

    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False

    # Azure SDKのログを抑制
    logging.getLogger("azure").setLevel(logging.WARNING)

    return logging.getLogger(logger_name) if logger_name else root

# ============================================================================
# ユーティリティ関数
# ============================================================================

def parse_resource_id(resource_id: str) -> Dict[str, str]:
    """
    Azure リソースIDをパースしてコンポーネントを抽出
    
    例: /subscriptions/{sub}/resourceGroups/{rg}/providers/{provider}/{type}/{name}
    """
    pattern = r"/subscriptions/(?P<subscription>[^/]+)/resourceGroups/(?P<resource_group>[^/]+)/providers/(?P<provider>[^/]+)/(?P<resource_type>[^/]+)/(?P<resource_name>[^/]+)"
    match = re.match(pattern, resource_id, re.IGNORECASE)
    
    if not match:
        raise ValueError(f"Invalid Azure resource ID format: {resource_id}")
    
    return match.groupdict()


def retry_with_backoff(logger: logging.Logger, config: Dict[str, Any]):
    """
    リトライデコレータ（指数バックオフ）
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            max_retries = config.get("max_retries", 3)
            initial_delay = config.get("initial_delay", 2)
            backoff_multiplier = config.get("backoff_multiplier", 2)
            max_delay = config.get("max_delay", 30)
            
            attempt = 0
            while attempt <= max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt > max_retries:
                        logger.error(f"{func.__name__} が {max_retries} 回のリトライ後に失敗しました: {str(e)}")
                        raise
                    
                    delay = min(initial_delay * (backoff_multiplier ** (attempt - 1)), max_delay)
                    logger.warning(f"{func.__name__} が失敗しました (試行 {attempt}/{max_retries}): {str(e)}")
                    logger.info(f"{delay} 秒後にリトライします...")
                    time.sleep(delay)
        
        return wrapper
    return decorator


# ============================================================================
# Azure操作関数
# ============================================================================

class AzureRunbookOperations:
    """Azure VM停止Runbook操作クラス"""
    
    def __init__(self, credential: DefaultAzureCredential, logger: logging.Logger, retry_config: Dict[str, Any]):
        self.credential = credential
        self.logger = logger
        self.retry_config = retry_config
        self.compute_clients: Dict[str, ComputeManagementClient] = {}
        self.monitor_clients: Dict[str, MonitorManagementClient] = {}
    
    def _get_compute_client(self, subscription_id: str) -> ComputeManagementClient:
        """サブスクリプション毎のComputeManagementClientを取得（キャッシュ）"""
        if subscription_id not in self.compute_clients:
            self.compute_clients[subscription_id] = ComputeManagementClient(
                credential=self.credential,
                subscription_id=subscription_id
            )
        return self.compute_clients[subscription_id]
    
    def _get_monitor_client(self, subscription_id: str) -> MonitorManagementClient:
        """サブスクリプション毎のMonitorManagementClientを取得（キャッシュ）"""
        if subscription_id not in self.monitor_clients:
            self.monitor_clients[subscription_id] = MonitorManagementClient(
                credential=self.credential,
                subscription_id=subscription_id
            )
        return self.monitor_clients[subscription_id]
    
    def disable_metric_alert(self, subscription_id: str, resource_group: str, alert_name: str) -> None:
        """メトリックアラートルールを無効化（リトライデコレータ付き）"""
        decorator = retry_with_backoff(self.logger, self.retry_config)
        
        @decorator
        def _disable():
            self.logger.info(f"メトリックアラートを無効化中: {alert_name}")
            client = self._get_monitor_client(subscription_id)
            alert = client.metric_alerts.get(resource_group, alert_name)
            alert.enabled = False
            client.metric_alerts.create_or_update(resource_group, alert_name, alert)
            self.logger.info(f"メトリックアラートの無効化に成功しました: {alert_name}")
        
        _disable()
    
    def disable_scheduled_query_rule(self, subscription_id: str, resource_group: str, rule_name: str) -> None:
        """ログアラートルール（Scheduled Query Rule）を無効化（リトライデコレータ付き）"""
        decorator = retry_with_backoff(self.logger, self.retry_config)
        
        @decorator
        def _disable():
            self.logger.info(f"ログアラートルールを無効化中: {rule_name}")
            client = self._get_monitor_client(subscription_id)
            rule = client.scheduled_query_rules.get(resource_group, rule_name)
            rule.enabled = False
            client.scheduled_query_rules.create_or_update(resource_group, rule_name, rule)
            self.logger.info(f"ログアラートルールの無効化に成功しました: {rule_name}")
        
        _disable()
    
    def disable_activity_log_alert(self, subscription_id: str, resource_group: str, alert_name: str) -> None:
        """アクティビティログアラートを無効化（リトライデコレータ付き）"""
        decorator = retry_with_backoff(self.logger, self.retry_config)
        
        @decorator
        def _disable():
            self.logger.info(f"アクティビティログアラートを無効化中: {alert_name}")
            client = self._get_monitor_client(subscription_id)
            alert = client.activity_log_alerts.get(resource_group, alert_name)
            alert.enabled = False
            client.activity_log_alerts.create_or_update(resource_group, alert_name, alert)
            self.logger.info(f"アクティビティログアラートの無効化に成功しました: {alert_name}")
        
        _disable()
    
    def verify_metric_alert_disabled(self, subscription_id: str, resource_group: str, alert_name: str) -> bool:
        """メトリックアラートが無効化されているか確認"""
        self.logger.info(f"メトリックアラートの無効化を確認中: {alert_name}")
        client = self._get_monitor_client(subscription_id)
        alert = client.metric_alerts.get(resource_group, alert_name)
        is_disabled = not alert.enabled
        
        if is_disabled:
            self.logger.info(f"確認完了: メトリックアラートは無効化されています: {alert_name}")
        else:
            self.logger.error(f"確認失敗: メトリックアラートがまだ有効です: {alert_name}")
        
        return is_disabled
    
    def verify_scheduled_query_rule_disabled(self, subscription_id: str, resource_group: str, rule_name: str) -> bool:
        """ログアラートルールが無効化されているか確認"""
        self.logger.info(f"ログアラートルールの無効化を確認中: {rule_name}")
        client = self._get_monitor_client(subscription_id)
        rule = client.scheduled_query_rules.get(resource_group, rule_name)
        is_disabled = not rule.enabled
        
        if is_disabled:
            self.logger.info(f"確認完了: ログアラートルールは無効化されています: {rule_name}")
        else:
            self.logger.error(f"確認失敗: ログアラートルールがまだ有効です: {rule_name}")
        
        return is_disabled
    
    def verify_activity_log_alert_disabled(self, subscription_id: str, resource_group: str, alert_name: str) -> bool:
        """アクティビティログアラートが無効化されているか確認"""
        self.logger.info(f"アクティビティログアラートの無効化を確認中: {alert_name}")
        client = self._get_monitor_client(subscription_id)
        alert = client.activity_log_alerts.get(resource_group, alert_name)
        is_disabled = not alert.enabled
        
        if is_disabled:
            self.logger.info(f"確認完了: アクティビティログアラートは無効化されています: {alert_name}")
        else:
            self.logger.error(f"確認失敗: アクティビティログアラートがまだ有効です: {alert_name}")
        
        return is_disabled
    
    def disable_alert_rule(self, alert_resource_id: str) -> None:
        """
        アラートルールを無効化（種類を自動判別）
        """
        parsed = parse_resource_id(alert_resource_id)
        subscription_id = parsed["subscription"]
        resource_group = parsed["resource_group"]
        provider = parsed["provider"]
        resource_type = parsed["resource_type"]
        resource_name = parsed["resource_name"]
        
        full_type = f"{provider}/{resource_type}".lower()
        
        if full_type == "microsoft.insights/metricalerts":
            self.disable_metric_alert(subscription_id, resource_group, resource_name)
        elif full_type == "microsoft.insights/scheduledqueryrules":
            self.disable_scheduled_query_rule(subscription_id, resource_group, resource_name)
        elif full_type == "microsoft.insights/activitylogalerts":
            self.disable_activity_log_alert(subscription_id, resource_group, resource_name)
        else:
            raise ValueError(f"サポートされていないアラートルールの種類です: {full_type}")
    
    def verify_alert_rule_disabled(self, alert_resource_id: str) -> bool:
        """
        アラートルールが無効化されているか確認（種類を自動判別）
        """
        parsed = parse_resource_id(alert_resource_id)
        subscription_id = parsed["subscription"]
        resource_group = parsed["resource_group"]
        provider = parsed["provider"]
        resource_type = parsed["resource_type"]
        resource_name = parsed["resource_name"]
        
        full_type = f"{provider}/{resource_type}".lower()
        
        if full_type == "microsoft.insights/metricalerts":
            return self.verify_metric_alert_disabled(subscription_id, resource_group, resource_name)
        elif full_type == "microsoft.insights/scheduledqueryrules":
            return self.verify_scheduled_query_rule_disabled(subscription_id, resource_group, resource_name)
        elif full_type == "microsoft.insights/activitylogalerts":
            return self.verify_activity_log_alert_disabled(subscription_id, resource_group, resource_name)
        else:
            raise ValueError(f"サポートされていないアラートルールの種類です: {full_type}")
    
    def stop_vm(self, vm_resource_id: str) -> None:
        """
        VMを停止（リトライデコレータ付き）
        """
        parsed = parse_resource_id(vm_resource_id)
        subscription_id = parsed["subscription"]
        resource_group = parsed["resource_group"]
        vm_name = parsed["resource_name"]
        
        decorator = retry_with_backoff(self.logger, self.retry_config)
        
        @decorator
        def _stop():
            self.logger.info(f"VMを停止中: {vm_name} (リソースグループ: {resource_group})")
            client = self._get_compute_client(subscription_id)
            
            # VM停止（非同期操作）
            poller = client.virtual_machines.begin_deallocate(resource_group, vm_name)
            self.logger.info(f"VM停止操作を開始しました: {vm_name}")
            
            # 完了待機
            poller.result()
            self.logger.info(f"VM停止操作が完了しました: {vm_name}")
        
        _stop()
    
    def verify_vm_stopped(self, vm_resource_id: str) -> bool:
        """
        VMが停止状態か確認
        """
        parsed = parse_resource_id(vm_resource_id)
        subscription_id = parsed["subscription"]
        resource_group = parsed["resource_group"]
        vm_name = parsed["resource_name"]
        
        self.logger.info(f"VMの停止状態を確認中: {vm_name}")
        client = self._get_compute_client(subscription_id)
        
        # VMのインスタンスビューを取得
        vm_instance_view = client.virtual_machines.instance_view(resource_group, vm_name)
        
        # PowerState を取得
        power_state = None
        if vm_instance_view.statuses:
            for status in vm_instance_view.statuses:
                if status.code and status.code.startswith('PowerState/'):
                    power_state = status.code
                    break
        
        if not power_state:
            self.logger.error(f"VMのPowerStateを取得できませんでした: {vm_name}")
            return False
        
        self.logger.info(f"VM現在の状態: {vm_name} - {power_state}")
        
        # 停止状態を判定（deallocated）
        if power_state == 'PowerState/deallocated':
            self.logger.info(f"確認完了: VMは割り当て解除状態です: {vm_name} ({power_state})")
            return True
        else:
            self.logger.error(f"確認失敗: VMが割り当て解除状態ではありません: {vm_name} ({power_state})")
            return False


# ============================================================================
# メイン処理
# ============================================================================

def print_execution_summary(execution_results: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    実行結果サマリを出力
    
    Returns:
        bool: 全て成功した場合True、失敗があった場合False
    """
    print("")
    print("=" * 80)
    print("VM停止Runbook 実行結果サマリ")
    print("=" * 80)
    
    start_time = execution_results["start_time"]
    end_time = execution_results["end_time"]
    duration = end_time - start_time
    
    print(f"実行開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')} JST")
    print(f"実行終了: {end_time.strftime('%Y-%m-%d %H:%M:%S')} JST")
    print(f"総処理時間: {str(duration).split('.')[0]}")
    print("")
    
    total_vms = len(execution_results["vms"])
    success_count = 0
    failed_count = 0
    total_alerts = 0
    success_alerts = 0
    failed_alerts = 0
    
    # VM別結果表示
    for idx, vm_result in enumerate(execution_results["vms"], 1):
        print("-" * 80)
        print(f"【VM {idx}/{total_vms}: {vm_result['vm_name']}】")
        print("-" * 80)
        
        # アラートルール結果
        print("  アラートルール無効化:")
        for alert in vm_result["alerts"]:
            total_alerts += 1
            status_icon = "✓" if alert["status"] == "success" else "✗"
            status_text = "無効化完了" if alert["status"] == "success" else f"無効化失敗 ({alert['error']})"
            print(f"    {status_icon} {alert['name']:<30s}: {status_text}")
            if alert["status"] == "success":
                success_alerts += 1
            else:
                failed_alerts += 1
        
        print("")
        
        # VM停止結果
        print("  VM割り当て解除:")
        vm_stop = vm_result["vm_stop"]
        if vm_stop["status"] == "success":
            print(f"    ✓ 実行完了 ({vm_stop['power_state']})")
        elif vm_stop["status"] == "skipped":
            print(f"    ⊘ スキップ ({vm_stop['error']})")
        else:
            print(f"    ✗ 失敗 ({vm_stop['error']})")
        
        print("")
        
        # VM処理結果
        overall_icon = "✓" if vm_result["overall_status"] == "success" else "✗"
        overall_text = "成功" if vm_result["overall_status"] == "success" else "失敗"
        vm_duration = vm_result["end_time"] - vm_result["start_time"]
        print(f"  処理結果: {overall_icon} {overall_text}")
        print(f"  処理時間: {str(vm_duration).split('.')[0]}")
        print("")
        
        if vm_result["overall_status"] == "success":
            success_count += 1
        else:
            failed_count += 1
    
    # 全体サマリ
    print("=" * 80)
    print("【全体サマリ】")
    
    success_vms = [vm["vm_name"] for vm in execution_results["vms"] if vm["overall_status"] == "success"]
    failed_vms = [vm["vm_name"] for vm in execution_results["vms"] if vm["overall_status"] == "failed"]
    
    print(f"  対象VM数: {total_vms}台")
    print(f"  成功: {success_count}台" + (f" ({', '.join(success_vms)})" if success_vms else ""))
    print(f"  失敗: {failed_count}台" + (f" ({', '.join(failed_vms)})" if failed_vms else ""))
    print("")
    print(f"  総アラートルール数: {total_alerts}件")
    print(f"  無効化成功: {success_alerts}件")
    print(f"  無効化失敗: {failed_alerts}件")
    print("")
    
    if failed_count == 0:
        print("  実行結果: ✓ 全て成功")
    else:
        print("  実行結果: ✗ 一部または全て失敗")
    
    print("=" * 80)
    
    return failed_count == 0


def main():
    """VM停止Runbookのメイン処理"""
    
    # ログ初期化
    logger = setup_stdout_logging(
        level=logging.INFO,
        fmt="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        force=True
    )
    logger = logging.getLogger("StopVMRunbook")
    
    logger.info("=" * 80)
    logger.info("VM停止Runbook開始")
    logger.info("=" * 80)
    
    # 実行結果記録用
    execution_results = {
        "start_time": datetime.now(JST),
        "end_time": None,
        "vms": []
    }
    
    try:
        # Azure認証
        logger.info("DefaultAzureCredentialを使用してAzureに認証中...")
        credential = DefaultAzureCredential()
        
        # 操作クラス初期化
        operations = AzureRunbookOperations(credential, logger, RETRY_CONFIG)
        
        # VM毎に処理
        total_vms = len(VM_ALERT_CONFIG)
        for idx, (vm_resource_id, alert_rules) in enumerate(VM_ALERT_CONFIG.items(), 1):
            # VM名を抽出
            vm_name = parse_resource_id(vm_resource_id)["resource_name"]
            
            # VM処理結果を記録
            vm_result = {
                "vm_name": vm_name,
                "start_time": datetime.now(JST),
                "end_time": None,
                "alerts": [],
                "vm_stop": {"status": "skipped", "power_state": None, "error": "未実行"},
                "overall_status": "failed"
            }
            
            logger.info("-" * 80)
            logger.info(f"VM処理中 {idx}/{total_vms}: {vm_name}")
            logger.info("-" * 80)
            
            try:
                # ステップ1: アラートルール無効化
                logger.info(f"ステップ1: {vm_name} のアラートルール {len(alert_rules)} 件を無効化中")
                for alert_idx, alert_resource_id in enumerate(alert_rules, 1):
                    alert_name = None
                    try:
                        alert_name = parse_resource_id(alert_resource_id)["resource_name"]
                        logger.info(f"  [{alert_idx}/{len(alert_rules)}] アラートを無効化中: {alert_name}")
                        operations.disable_alert_rule(alert_resource_id)
                        vm_result["alerts"].append({"name": alert_name, "status": "success", "error": None})
                    except Exception as e:
                        if not alert_name:
                            alert_name = parse_resource_id(alert_resource_id).get("resource_name", alert_resource_id)
                        error_msg = str(e).split('\n')[0]  # 最初の行のみ
                        vm_result["alerts"].append({"name": alert_name, "status": "failed", "error": error_msg})
                        logger.error(f"  アラートルールの無効化に失敗しました: {alert_name}")
                        logger.exception(e)
                        raise  # アラート無効化失敗時は VM停止をスキップ
                
                # ステップ2: アラート無効化確認
                logger.info(f"ステップ2: {vm_name} のすべてのアラートルールの無効化を確認中")
                all_disabled = True
                for alert_idx, alert_resource_id in enumerate(alert_rules, 1):
                    alert_name = None
                    try:
                        alert_name = parse_resource_id(alert_resource_id)["resource_name"]
                        logger.info(f"  [{alert_idx}/{len(alert_rules)}] アラートを確認中: {alert_name}")
                        is_disabled = operations.verify_alert_rule_disabled(alert_resource_id)
                        if not is_disabled:
                            all_disabled = False
                            logger.error(f"  アラートルールがまだ有効です: {alert_name}")
                            # アラート結果を更新（確認失敗）
                            for alert in vm_result["alerts"]:
                                if alert["name"] == alert_name:
                                    alert["status"] = "failed"
                                    alert["error"] = "無効化確認に失敗"
                                    break
                    except Exception as e:
                        if not alert_name:
                            alert_name = parse_resource_id(alert_resource_id).get("resource_name", alert_resource_id)
                        logger.error(f"  アラートルールの確認に失敗しました: {alert_name}")
                        logger.exception(e)
                        all_disabled = False
                        # アラート結果を更新（確認エラー）
                        for alert in vm_result["alerts"]:
                            if alert["name"] == alert_name:
                                alert["status"] = "failed"
                                alert["error"] = "確認エラー"
                                break
                
                if not all_disabled:
                    vm_result["vm_stop"]["status"] = "skipped"
                    vm_result["vm_stop"]["error"] = "アラート無効化に失敗したため"
                    logger.error(f"{vm_name} の一部のアラートルールがまだ有効です。VM停止をスキップします。")
                    continue
                
                logger.info(f"{vm_name} のすべてのアラートルールの無効化を確認しました")
                
                # ステップ3: VM停止
                logger.info(f"ステップ3: VM {vm_name} を停止中")
                operations.stop_vm(vm_resource_id)
                
                # ステップ4: VM停止確認
                logger.info(f"ステップ4: VM {vm_name} の停止状態を確認中")
                if not operations.verify_vm_stopped(vm_resource_id):
                    vm_result["vm_stop"]["status"] = "failed"
                    vm_result["vm_stop"]["error"] = "停止状態の確認に失敗"
                    raise Exception(f"VM {vm_name} が停止状態ではありません")
                
                # VM停止成功
                # PowerStateを取得
                parsed = parse_resource_id(vm_resource_id)
                client = operations._get_compute_client(parsed["subscription"])
                vm_instance_view = client.virtual_machines.instance_view(parsed["resource_group"], parsed["resource_name"])
                power_state = None
                if vm_instance_view.statuses:
                    for status in vm_instance_view.statuses:
                        if status.code and status.code.startswith('PowerState/'):
                            power_state = status.code
                            break
                
                vm_result["vm_stop"]["status"] = "success"
                vm_result["vm_stop"]["power_state"] = power_state or "不明"
                vm_result["vm_stop"]["error"] = None
                vm_result["overall_status"] = "success"
                
                logger.info(f"{vm_name} の処理が正常に完了しました")
                
            except Exception as e:
                logger.error(f"{vm_name} の処理に失敗しました: {str(e)}")
                logger.exception(e)
                logger.warning(f"エラーのため {vm_name} のVM停止をスキップします")
                vm_result["overall_status"] = "failed"
            finally:
                vm_result["end_time"] = datetime.now(JST)
                execution_results["vms"].append(vm_result)
        
        # 実行終了時刻を記録
        execution_results["end_time"] = datetime.now(JST)
        
        # 実行結果サマリを出力
        all_success = print_execution_summary(execution_results, logger)
        
        # 失敗があれば例外を発生
        if not all_success:
            raise Exception("VM停止処理の異常を検知しました。")
        
    except Exception as e:
        logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
