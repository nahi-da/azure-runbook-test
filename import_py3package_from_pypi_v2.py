import os
import sys
import subprocess
import argparse
import time
import traceback
import json
import urllib.request
import urllib.error
import zipfile
import email.parser
import datetime

#################################################
# ▼▼▼ パラメーター定義（ここから） ▼▼▼
#################################################

# テストモード
DEVMODE = True

# サブスクリプションID
SUCSCRIPTION_ID = ""

# リソースグループ名
RG_NAME = ""

# Automationアカウント名
AA_NAME = ""

# ランタイム環境名
RUNTIME_ENV = ""

# ランタイム環境にPythonパッケージを追加するためのAPIエンドポイント
API_ENDPOINT = "https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Automation/automationAccounts/{}/runtimeEnvironments/{}/packages/{}?api-version={}"

# 上記APIのバージョン
API_VER = "2024-10-23"

# ループ処理でAPIを呼び出すときの待機時間（秒）
WAIT_TIME = 10

# whlファイルの一時ダウンロード先
PIP_DL_DIR = "./pydl-" + datetime.datetime.now().strftime(r"%y%m%d%H%M%S")

#################################################
# ▲▲▲ パラメーター定義（ここまで） ▲▲▲
#################################################


def get_access_token() -> str:
    """
    Azure Managed Identity を使用してアクセストークンを取得
    Returns: token
    """

    endpoint = os.getenv('IDENTITY_ENDPOINT')
    identity_header = os.getenv('IDENTITY_HEADER')

    if not endpoint or not identity_header:
        raise Exception("Managed Identity環境変数が取得できませんでした。")
    
    url = f"{endpoint}?resource=https://management.azure.com/"
    headers = {
        'X-IDENTITY-HEADER': identity_header,
        'Metadata': 'True'
    }
    
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data['access_token']
    except urllib.error.URLError as e:
        raise Exception("アクセストークンの取得に失敗しました") from e
    except (KeyError, json.JSONDecodeError) as e:
        raise Exception("トークンレスポンスの解析に失敗しました") from e


def send_webservice_import_module_request(packagename, download_uri_for_file, subscription_id, resource_group, automation_account, runtime_env, token):
    """
    Azure Automation APIを使用してパッケージをインポート
    """
    request_url = API_ENDPOINT.format(subscription_id, resource_group, automation_account, runtime_env, packagename, API_VER)
    requestbody = {
        'properties': {
            'description': 'uploaded via automation',
            'contentLink': {'uri': download_uri_for_file}
        }
    }
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    try:
        req = urllib.request.Request(
            request_url,
            data=json.dumps(requestbody).encode('utf-8'),
            headers=headers,
            method='PUT'
        )
        with urllib.request.urlopen(req) as response:
            status_code = response.status
            return status_code
    except Exception as e:
        raise Exception(f"  {packagename}: インポートエラー") from e


def get_package_info_from_wheel(wheel_path) -> tuple:
    """
    whlファイルからパッケージ情報を取得
    Returns: (package_name, version)
    """
    try:
        with zipfile.ZipFile(wheel_path, 'r') as whl:
            # METADATA または PKG-INFO を探す
            metadata_file = None
            for name in whl.namelist():
                if name.endswith('METADATA') or name.endswith('PKG-INFO'):
                    metadata_file = name
                    break
            
            if not metadata_file:
                print(f"Error: {wheel_path} の解析に失敗しました。")
                sys.exit(1)
            
            # メタデータを解析
            metadata_content = whl.read(metadata_file).decode('utf-8')
            parser = email.parser.Parser()
            metadata = parser.parsestr(metadata_content)
            
            package_name = metadata.get('Name')
            version = metadata.get('Version')
            
            return package_name, version
    except Exception as e:
        print("".join(traceback.TracebackException.from_exception(e).format()))
        print(f"Error: {wheel_path} の解析に失敗しました。")
        sys.exit(1)


def get_package_details(packages: list) -> list:
    """
    pipで依存関係込みのパッケージを取得し、詳細情報を返す関数
    Returns: (package_name, version, package_filename)
    """
    #################################################
    # pipを使用してパッケージをダウンロード
    #################################################
    try:
        res = subprocess.run(
            [sys.executable, '-m', 'pip', 'download'] + packages + ['--disable-pip-version-check', '-d', PIP_DL_DIR],
            text=True,
            capture_output=True,
            timeout=300  # タイムアウト: 5分
        )
    # Python例外処理
    except Exception as e:
        print("".join(traceback.TracebackException.from_exception(e).format()))
        print("Error: pip実行時にエラーが発生しました。")
        sys.exit(1)
    # 終了コードによる例外処理
    if res.returncode != 0:
        print(res.stderr)
        print("Error: pip実行時にエラーが発生しました。")
        sys.exit(1)
    
    #################################################
    # ダウンロードしたファイルのファイル名を保存
    #################################################
    file_details = list()
    try:
        for root, dirs, files in os.walk(PIP_DL_DIR):
            for file in files:
                if ".whl" in file:
                    print("  Found:", os.path.join(root, file))
                    # whlファイルを解析してパッケージ情報を取得する
                    package_info = get_package_info_from_wheel(os.path.join(root, file))
                    file_details.append(package_info + (file,))
    # Python例外処理
    except Exception as e:
        print("".join(traceback.TracebackException.from_exception(e).format()))
        print("Error: whlファイルの走査中にエラーが発生しました。")
        sys.exit(1)

    return file_details


def get_package_url(package_name, version, filename):
    """
    パッケージのダウンロードURLを構築
    Return: url
    """
    api_url = f"https://pypi.org/pypi/{package_name}/{version}/json"
    
    try:
        req = urllib.request.Request(api_url)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            # urlsから該当ファイルを探す
            for url_info in data.get('urls', []):
                if url_info.get('filename') == filename:
                    return str(url_info.get('url'))
            else:
                return ""
    except Exception as e:
        print("".join(traceback.TracebackException.from_exception(e).format()))
        print("Error: PyPI APIからURL取得中にエラーが発生しました。")
        sys.exit(1)


def main():
    """
    メイン処理
    """

    #################################################
    # argparseで引数をパース
    #################################################
    parser = argparse.ArgumentParser(
        description='Azure AutomationアカウントにPython 3パッケージと依存関係をインポート',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
引数（位置指定パラメーター）の例:
  # バージョンを指定する場合
  -s xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxx
  -g contosogroup
  -a contosoaccount
  -r python310
  numpy==1.26
  requests>=2.0 
  
  # 最新版をインストールする場合（バージョン省略）
  -s xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxx
  -g contosogroup
  -a contosoaccount
  -r python310
  numpy
  requests
'''
    )

    #################################################
    # 引数の設定
    #################################################
    parser.add_argument('-s', '--subscription-id', default=SUCSCRIPTION_ID,
                        help='AzureサブスクリプションID')
    parser.add_argument('-g', '--resource-group', default=RG_NAME,
                        help='Automationアカウントのリソースグループ名')
    parser.add_argument('-a', '--automation-account', default=AA_NAME,
                        help='Automationアカウント名')
    parser.add_argument('-r', '--runtime-env', default=RUNTIME_ENV,
                        help='ランタイム環境名 (例: python310)')
    parser.add_argument("packages", nargs="*")
    args = parser.parse_args()
    
    #################################################
    # 入力内容の出力
    #################################################
    # パッケージ未指定時はエラーで終了する
    if not args.packages:
        print("Error: パッケージ名が未指定です。")
        sys.exit(1)
    
    print("=" * 60)
    print(f"サブスクリプション: {args.subscription_id}")
    print(f"リソースグループ: {args.resource_group}")
    print(f"Automationアカウント: {args.automation_account}")
    print(f"ランタイム環境: {args.runtime_env}")
    print("パッケージ:\n{}".format("\n".join(["  - " + pkg for pkg in args.packages])))
    print("=" * 60)

    #################################################
    # pipモジュールが使用可能か確認
    #################################################
    print("")
    print("Info: [1/4] pipモジュールの確認")
    try:
        import pip
    except:
        print("Error: pipモジュールのインポートに失敗しました。\n       仕様変更により利用できなくなった可能性があります。")
        sys.exit(1)
    print("  pipモジュールが使用できます。")
    
    #################################################
    # パッケージの詳細情報を依存関係にあるパッケージ込みで取得する
    #################################################
    print("")
    print("Info: [2/4] パッケージ情報の取得")
    package_details = get_package_details(args.packages)

    #################################################
    # アクセストークン取得
    #################################################
    print("")
    print("Info: [3/4] アクセストークンの取得")
    if DEVMODE:
        access_token = ""
        print("  開発モードが有効です。認証をスキップします。")
    else:
        try:
            access_token = get_access_token()
            print("  認証成功")
        except Exception as e:
            print("".join(traceback.TracebackException.from_exception(e).format()))
            print("Error: アクセストークンの取得でエラーが発生しました。")
            sys.exit(1)

    #################################################
    # ランタイム環境にパッケージをインポート
    #################################################
    fail_pkgs = list()
    print("")
    print("Info: [4/4] パッケージをランタイム環境にインポート")
    for idx, (pkg_name, pkg_ver, pkg_filename) in enumerate(package_details, start=1):
        print("  [{}/{}] {} (v{})".format(idx, len(package_details), pkg_name, pkg_ver))
        # パッケージダウンロード先URLを構築
        pkg_url = get_package_url(pkg_name, pkg_ver, pkg_filename)
        if pkg_url:
            print("    URL構築完了")
            if DEVMODE:
                print("    開発モードが有効です。インポートをスキップします。")
            else:
                # パッケージのインポート要求送信
                try:
                    status = send_webservice_import_module_request(
                        packagename=pkg_name,
                        download_uri_for_file=pkg_url,
                        subscription_id=args.subscription_id,
                        resource_group=args.resource_group,
                        automation_account=args.automation_account,
                        runtime_env=args.runtime_env,
                        token=access_token
                    )
                    
                    print("    インポート成功")
                except Exception as e:
                    print("".join(traceback.TracebackException.from_exception(e).format()))
                    print("    インポート失敗")
                    fail_pkgs.append((pkg_name, pkg_ver, pkg_filename))

        # 次のパッケージに移る前にウエイトを挟む
        print(f"    APIアクセス制限防止のため、{WAIT_TIME}秒待機しています。しばらくお待ちください。")
        time.sleep(WAIT_TIME)
        print()
    
    #################################################
    # 結果サマリー
    #################################################
    print("")
    print("=" * 60)

    print("インポート成功:")
    print("\n".join([f"  - {pkgs[0]} (v{pkgs[1]})\n    {pkgs[2]}" for pkgs in package_details if pkgs not in fail_pkgs]))
    
    if len(fail_pkgs) > 0:
        print("インポート失敗:")
        print("\n".join([f"  - {pkgs[0]} (v{pkgs[1]})\n    {pkgs[2]}" for pkgs in fail_pkgs]))

    print("=" * 60)

if __name__ == "__main__":
    main()