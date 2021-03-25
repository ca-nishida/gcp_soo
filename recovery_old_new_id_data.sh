#!/bin/sh
# old_new_id_sourceへのロード方式をWRITE_APPENDにしておくこと
# prd環境の本番データをdevのGCSコピーすることで、dev環境で作業を実施する。

#------------------------------
IFS_ORG=$IFS
IFS_LF=$'\n'
bucket='jbtob-pos-from-sybase-prd' #コピー元GCSバケット名
object='data/diff_old_new_id_*' #対象object
target_bucket='jbtob-pos-from-sybase-dev' #コピーバケット先
target_folder='data'　
#------------------------------


which bq >/dev/null
if [ $? -ne 0 ]; then
	echo "this script require bq." >/dev/stderr
	exit 1
fi

which gsutil >/dev/null
if [ $? -ne 0 ]; then
	echo "this script require gsutil." >/dev/stderr
	exit 2
fi

#ロード対象のgcsパスを取得
source_object=$(gsutil ls gs://${bucket}/${object})

#ロード前のold_new_id_sourceの列数を取得、条件分岐の都合上、1にしている
row_count_before=1
#ロード後のold_new_id_sourceの列数
row_count_after=0
for so in ${source_object}; do
  IFS=${IFS_ORG}
  echo "load file: $so"
  # GCSに対象ファイルをロードし、bq load実行
  gsutil cp ${so} gs://${target_bucket}/${target_folder}/

  i=1
  # ロードが終わるまで待機
  while [ $row_count_before -ge $row_count_after ] #$row_count_before >= $row_count_after
	do	
		echo "Check Job Count: $i"
		#old_new_id_sourceテーブルの列数取得
		row_count_after=`bq show jbtob-looker-soo-dev:looker.old_new_id_source | sed -n 5P | awk '{print $8}'`
		i=$(( i + 1 )) 
		#ロード完了チェックが50回実行されたらbreak
		if [ $i -eq 50 ]; then
			echo "load fail. file name: $so"
			break
		fi
		
	done
  # ロード前とロード後の列数の比較
  echo "load job complete. before:$row_count_before, after:$row_count_after"
  
  echo "strat proprecessing"
  # old_new_idのプロシージャを直接叩かない理由は、old_new_idの中間テーブルであるold_new_id_sourceに対して前処理を実施するため。
  # プロシージャではold_new_idに対して、前処理を実施しているが今回のリカバリー作業はold_new_id_sourceに対して行う。
  # そのためプロシージャのテーブル名部分だけを変更したものを使用する。

  # old_new_id_sourceから、old_new_idを作成する時に重複を排除している。
  # 今回の作業はold_new_id_sourceで実施するため、old_new_id_sourceを重複排除した状態で作成しなおしている。
  bq query --use_legacy_sql=false """
  	  CREATE OR REPLACE TABLE
      jbtob-looker-soo-dev.looker.old_new_id_source AS(
      SELECT DISTINCT
		old_ID,
		new_ID,
		dummy
      FROM
      	jbtob-looker-soo-dev.looker.old_new_id_source);
  """

  # old_new_idの前処理1つ目
  bq query --use_legacy_sql=false """MERGE jbtob-looker-soo-dev.looker.old_new_id_source id1
      USING jbtob-looker-soo-dev.looker.old_new_id_source id2
        ON id1.old_ID = id2.old_ID 
      WHEN MATCHED AND id1.new_ID < id2.new_ID
      THEN
      DELETE;
  """

  # old_new_idの前処理2つ目
  bq query --use_legacy_sql=false """MERGE jbtob-looker-soo-dev.looker.old_new_id_source id1
      USING jbtob-looker-soo-dev.looker.old_new_id_source id2
        ON id1.new_ID = id2.old_ID 
      WHEN MATCHED
      THEN
      UPDATE SET
      id1.new_ID = id2.new_ID;
  """

  # old_new_idの前処理2つ目の2回目、2段階のID書き換えに対応するために実施
  bq query --use_legacy_sql=false """MERGE jbtob-looker-soo-dev.looker.old_new_id_source id1
      USING jbtob-looker-soo-dev.looker.old_new_id_source id2
        ON id1.new_ID = id2.old_ID 
      WHEN MATCHED
      THEN
      UPDATE SET
      id1.new_ID = id2.new_ID;
  """

  echo "finish proprecessing"

  #old_new_id_sourceテーブルの列数取得
  #重複排除で列数が減少する可能性があるので取得し直している。
  row_count_after=`bq show jbtob-looker-soo-dev:looker.old_new_id_source | sed -n 5P | awk '{print $8}'`
  row_count_before=$row_count_after

done