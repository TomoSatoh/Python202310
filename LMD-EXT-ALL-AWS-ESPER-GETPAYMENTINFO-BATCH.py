import ssl
import json
import csv
import boto3
from botocore.config import Config
import os
import pandas as pd
import platform
import datetime
import hashlib
import logger_config as logger_config
logger = logger_config.set_logger(__name__)
import requests
import traceback

# SSM PARAMETER からパラメーター取得
def db_secret_information():
    region = os.environ['AWS_REGION']
    env = os.environ['ENV']

    logger.info("SSM PARAMETER からパラメーター取得開始")

    #収納企業番号、APIパスワードを取得
    try:
        ssm = boto3.client(
            "ssm", endpoint_url=f"https://ssm.{region}.amazonaws.com",
                  config=Config(connect_timeout=5,
                                read_timeout=5,
                                retries={"mode": "standard",
                                         "total_max_attempts": 3,}
                               )
            )
        response = ssm.get_parameters(
            Names=[f"{env}_ESPER_API_KEY"], WithDecryption=True)

        parameters = response['Parameters'][0]["Value"]
        parameter_json = json.loads(parameters)

        receipt_company_number = parameter_json['RECEIPT_COMPANY_NUMBER']
        api_password = parameter_json['API_PASSWORD']

    except Exception as e:
        logger.error(f"!! SSM PARAMETERからのパラメーター取得失敗 !!:{e}")
        traceback.print_exc()
        raise e

    return receipt_company_number,api_password

# WebAPIにリクエストをポストする
def send_request(pr_data_key, pr_completion_flg, request_time):
    dblq = '\"'
    dblq_cln = '\":\"'
    dblq_com = '\",'

    try:
        logger.info(f"!! WebAPIにリクエストをポストする処理開始!!")
        # ヘッダの値のセット
        host = os.environ['REQUEST_HEADER_HOST']
        # 送信元ユーザーエージェント名 
        useragent = os.environ['REQUEST_HEADER_USERAGENT']
    
        # リクエスト（１回目）の値のセット
        # 収納企業番号、APIパスワードをSSM PARAMETER から取得
        company_bn, api_password = db_secret_information()

        if pr_data_key == '':
            # 初回のリクエスト
            # 取得対象日（From） 処理日付の180日（最大保管日数分）
            ymd_from = str((request_time - datetime.timedelta(180)).strftime("%Y%m%d"))
            # 取得対象日（To）   処理日付
            ymd_to = str(request_time.strftime("%Y%m%d"))
            # 取得条件   0：全て 1:未取得分のみ  本番は1:未取得分のみを設定する
            get_conditions = '1'
            
        else:
            # 二回目以降のリクエスト
            # 取得対象日（From） 処理日付の180日（最大保管日数分）
            ymd_from = ''
            # 取得対象日（To）   処理日付
            ymd_to = ''
            # 取得条件
            get_conditions = ''
    
        # データキー
        data_key = pr_data_key
        # リクエスト依頼時間
        request_time = request_time.strftime('%Y%m%d%H%M%S'+'000'+'%f')[:-3]

        # 連携完了フラグ
        completion_flg = pr_completion_flg
        
        # URLセット
        url = os.environ['REQUEST_URL']
        
        # リクエスト（１回目）のＡＰＩパラメータ作成　ハッシュ値セット
        # ハッシュ値計算
        sha_str = company_bn + ymd_from + ymd_to + \
            get_conditions + data_key + request_time + completion_flg + api_password

        # リクエストのＡＰＩパラメータ作成
        api_param = '{'
        api_param += dblq + 'COMPANY_BN' + dblq_cln + company_bn + dblq_com
        api_param += dblq + 'YMD_FROM' + dblq_cln + ymd_from + dblq_com
        api_param += dblq + 'YMD_TO' + dblq_cln + ymd_to + dblq_com
        api_param += dblq + 'GET_CONDITIONS' + dblq_cln + get_conditions + dblq_com
        api_param += dblq + 'DATA_KEY' + dblq_cln + data_key + dblq_com
        api_param += dblq + 'REQUEST_TIME' + dblq_cln + request_time + dblq_com
        api_param += dblq + 'COMPLETION_FLG' + dblq_cln + completion_flg + dblq_com
        cert_key = hashlib.sha256(sha_str.encode()).hexdigest()
        api_param += dblq + 'CERT_KEY' + dblq_cln + cert_key + dblq
        api_param += '}'
        
        #ヘッダ セット
        api_header = {
        'Host':host,
        'Content-type':'application/json',
        'Content-length':len(api_param),
        'User-agent':useragent,
        'Connection':'close'
        }

        # WebAPIへPOST
        request_post = requests.post(url, api_param, api_header)

        logger.info(request_post.json())

    except Exception as e:
        logger.error(f"!! WebAPIにリクエストをポスト処理失敗!!:{e}")
        traceback.print_exc()
        raise e

    finally:
        logger.info(f"!! WebAPIにリクエストをポストする処理終了!!")
        # レスポンスをRETURN
        return request_post.json() 

# ＷｅｂＡＰＩのレスポンス保存ファイル作成
def wapifile_make(pr_line_str, wapi_file):
    s3bucketname = os.environ['S3_BUCKETNAME']

    try:
        logger.info(f"!! ＷｅｂＡＰＩのレスポンス保存ファイル作成処理開始!!")
        # 120Byreづつでカット
        split_str = [pr_line_str[x:x + 120] for x in range(0, len(pr_line_str), 120)]
        
        #  空のリスト
        ouput_list = []
        
        # txtファイルを整理する
        for split_str_line in split_str:
            ouput_list.append('\"' + split_str_line + '\"' + '\n')
        
        # tmpへ出力する
        with open(wapi_file, 'a', encoding='utf-8', newline='\r\n') as outfile:
            outfile.writelines(ouput_list)
    
    except Exception as e:
        logger.error(f"!! ＷｅｂＡＰＩのレスポンス保存ファイル作成処理失敗!!:{e}")
        traceback.print_exc()
        raise e
    
    finally:
        logger.info(f"!! ＷｅｂＡＰＩのレスポンス保存ファイル作成処理終了!!")
        return

# 連携ファイルを作成
def aligfile_make(wapi_file, alig_file):

    try:
        logger.info(f"!! 連携ファイルを作成処理開始!!")
        # txtファイルを読み込み
        infile = open(wapi_file, 'r', encoding='utf-8')
        line = infile.readline()
        infile.close
        
        # txtファイルを読み込み
        infile = wapi_file
        colspecs = [(0,1), (1,2), (2,4), (4,12), (12,16), (16,18), (18,19),
                    (19,24), (24,29), (29,45), (45,46), (46,52), (52,53),
                    (53,59), (59,60), (60,63), (63,70), (70,78), (78,86),
                    (86,94), (94,102), (102,106), (106,107), (107,119), 
                    (119,121), (121,122)
                    ]
        names=['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                'U', 'V', 'W', 'X', 'Y', 'Z']
        df = pd.read_fwf(infile, colspecs=colspecs, names=names, header=None,
                         dtype={2: str, 3: str, 4: str, 5: str, 6: str,
                                7: str, 8: str, 9: str, 10: str, 11: str, 12: str,
                                13: str, 14: str, 15: str, 16: str, 17: str, 18: str,
                                19: str, 20: str, 21: str, 22: str, 23: str, 24: str,
                                25: str
                         })    
    
        #ダブルクォーテーションの列を削除
        df.drop(columns = ['A', 'Z'], inplace=True)
    
        #レコード区分=2を残す
        df = df[df['B'] == 2]
        
        # csvへ出力する
        df.to_csv(alig_file , encoding="utf-8", 
                  index = False, header = False, quotechar='"', quoting=csv.QUOTE_ALL)
    
    except Exception as e:
        logger.error(f"!! 連携ファイルを作成処理失敗!!:{e}")
        traceback.print_exc()
        raise e
    
    finally:
        logger.info(f"!! 連携ファイルを作成処理終了!!")
        return
    
# 作成したCSVファイルをS3に配置
def csv_S3(makefile, s3fle):
    s3bucketname = os.environ['S3_BUCKETNAME']

    try:
        logger.info(f"!! CSVファイルを作成してS3に配置処理開始!!")

        # S3アップロード
        s3 = boto3.client('s3',
                          config=Config(connect_timeout=5,
                                        read_timeout=5,
                                        retries={"mode": "standard",
                                                 "total_max_attempts": 3,}
                                       )
                         )
        s3.upload_file(makefile, s3bucketname, s3fle)
        
    except s3.exceptions.BucketAlreadyExists as e:
        # バケット名は既に所有されています エラー発生時
        logger.error(f'S3 bucket already exists: {e.response["Error"]["BucketName"]}')
    
    except s3.exceptions.NoSuchKey as e:
        # オブジェクト（ファイル）を取得するために指定したキーが存在しない
        logger.error(f'No such key: {e.response["Error"]["Key"]}')
    
    except s3.exceptions.NoSuchEntityException as e:
        # ユーザー、グループ、ロール、またはポリシーが削除された
        logger.error(e.response['Error']['Message'])
    
    except s3.exceptions.AlreadyExistsException as e:
        # 指定されたエンティティが存在しない
        logger.error(e.response['Error']['Message'])

    except Exception as e:
        logger.error(f"!! CSVファイルを作成してS3に配置処理失敗!!:{e}")
        traceback.print_exc()
        raise e
    
    finally:
        logger.info(f"!! CSVファイルを作成してS3に配置処理終了!!")
        # tmpフォルダのファイルを削除
        os.remove(makefile)
        return

def lambda_handler(event, context):

    try:
        logger.info(f"!! 処理開始(新エスパー 入金情報取得処理) !!")
    
        # リクエスト時間設定
        t_delta = datetime.timedelta(hours=9)  # 9時間
        JST = datetime.timezone(t_delta, 'JST')  # UTCから9時間差の「JST」タイムゾーン
        request_time = datetime.datetime.now(JST)  # タイムゾーン付きでローカルな日付と時刻を取得
        outpdatetime = request_time.strftime('%Y-%m-%d-%H-%M-%S') 
        wapifilename = 'wapifile'
        aligfilename = 'BBP108W'
        filextension = '.txt'


        # リクエスト（１回目）の値のセット
        # データキー 初回は設定不要
        data_key = ''
        # 連携完了フラグ 0：連携未完了
        completion_flg = '0'
    
        # WebAPIにリクエストをポストする
        ret_json = send_request(data_key, completion_flg, request_time)
    
        # １日の初回のリクエストは上記の処理でポストしている
        # while ループの処理内に入り（while_cnt = 1回目）で初回分の判定
        # データ無時は、while処理で完了リクエストをポストし、（while_cnt = 2回目）で終了
        # データ３００万件以下の時は、while処理で完了リクエストをポストし、（while_cnt = 2回目）で終了
        # データ３０１万件～６００万件の時は、while処理で２回リクエストをポストし、（while_cnt = 3回目）で終了
        # データ６０１万件～９００万件の時は、while処理で３回リクエストをポストし、（while_cnt = 4回目）で終了
        # 実質的に運用では３００万件以下なので（while_cnt = 2回目）で終了するが、バッファを持たせて５回で処理終了する
    
        # while処理カンター
        while_cnt = 0
        while while_cnt < 5:
            # while処理カンターアップ
            while_cnt += 1
    
            # レスポンスデータ解体（判定用）
            res_status = ret_json['STATUS']
            res_data_flg = ret_json['DATA_FLG']
    
            # レスポンス判定
            # ステータス=9 => エラー発生時 ==> エラー処理して終了
            if res_status == '9':
                logger.info(f"!! 処理終了条件成立(res_status = 9) !!")
                logger.info(f"!! 処理終了条件成立("+ ret_json['ERROR_CD'] + ") !!")
                logger.info(f"!! 処理終了条件成立("+ ret_json['ERROR_MESSAGE'] + ") !!")
                raise ValueError("res_status = 9:エラー " + ret_json['ERROR_CD'] + ":" + ret_json['ERROR_MESSAGE'] + "!!")
                break
    
            # ステータス=8 => 処理中時 ==> エラー処理して終了
            elif res_status == '8':
                logger.info(f"!! 処理終了条件成立(res_status = 8:処理中) !!")
                raise ValueError("res_status = 8:処理中!!")
                break
    
            # ステータス=0 & 未連携データ有無フラグ=3 ==> データ無① => データ無完了連携
            # ステータス=0 & 未連携データ有無フラグ=2 ==> 正常終了時② => データ無完了連携
            elif res_status == '0' and (res_data_flg == '3' or res_data_flg == '2'):
                logger.info(f"!! 処理継続条件成立(res_status = 0-3_2) !!")
                # データキー
                data_key = ret_json['DATA_KEY']  #レスポンスから取得
                # 収納データ
                receipt_data = ret_json['RECEIPT_DATA']  #レスポンスから取得
                # 連携完了フラグ 1:連携完了
                completion_flg = '1'
    
            # ステータス=0 & 未連携データ有無フラグ=1 => 正常終了時① ==>データ継続連携
            elif res_status == '0' and res_data_flg == '1':
                logger.info(f"!! 処理継続条件成立(res_status = 0-1) !!")
                # データキー
                data_key = ret_json['DATA_KEY']  #レスポンスから取得
                # 収納データ
                receipt_data = ret_json['RECEIPT_DATA']  #レスポンスから取得
                # 連携完了フラグ 0：連携未完了
                completion_flg = '0'
    
            # ステータス=0 & 未連携データ有無フラグ=0 => データ無②＆正常終了時③ ==> 処理正常終了
            elif res_status == '0' and res_data_flg == '0':
                logger.info(f"!! 処理終了条件成立(res_status = 0-0) !!")
                break
    
            # 未連携データ有無フラグ=1 or 2　の時に携データ作成処理を実施
            if (res_data_flg == '1' or res_data_flg == '2'):
                # ＷｅｂＡＰＩのレスポンス保存ファイル作成
                wapifile_make(receipt_data, '/tmp/' + wapifilename + filextension)
    
                # 連携ファイルを作成
                aligfile_make('/tmp/' + wapifilename + filextension, '/tmp/' + aligfilename + filextension)
    
                # 作成したCSVファイル(ＷｅｂＡＰＩのレスポンス保存ファイル)をS3に配置
                csv_S3('/tmp/' + wapifilename + filextension, 'webapi-file/' + wapifilename + '_' + outpdatetime + filextension)
    
                # 作成したCSVファイル(連携ファイル)をS3に配置
                csv_S3('/tmp/' + aligfilename + filextension, 'alignment/' + aligfilename + '_' + outpdatetime + filextension)

            # WebAPIにリクエストをポストする
            ret_json = send_request(data_key, completion_flg, request_time)

        logger.info(f"!! 処理終了(新エスパー 入金情報取得処理) !!")

        return "SUCCESS" 

    except Exception as e:
        logger.error(f"###### !! 異常終了 !! ######:{e}")
        traceback.print_exc()

        # 本ロジックのエラー時に完了リクエストがポストされていない可能性を考慮してポストする
        # データキー
        data_key = ret_json['DATA_KEY']  #レスポンスから取得
        # 連携完了フラグ 1:連携完了
        completion_flg = '1'
        # WebAPIにリクエストをポストする
        ret_json = send_request(data_key, completion_flg, request_time)

        raise e
