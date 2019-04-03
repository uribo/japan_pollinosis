日本の花粉データ
===============

日本国内の花粉観測により得られたデータを収集、整形するRコードを用意しています。対象地域およびデータソースは、環境省 [環境省花粉観測システム](http://kafun.taiki.go.jp/)において全国各地、東京都福祉保健局 [東京都アレルギー情報navi](http://www.tokyo-eiken.go.jp/kj_kankyo/kafun/) から東京都となります。

データの利用に関しては、各データソースの利用規約等に従ってください。

## Files

`data-raw/` フォルダ中ののファイルを実行すると各データソースが提供する過去データを取得し、Rで扱いやすい形式 (整形されたdata.frame、csvファイルとして出力) に変換されます。

- `data-raw/collect_fukushihoken_metro_tokyo-archives_with_scraping.R`... 東京都、2001年から2019年、スギおよびヒノキ
- `data-raw/download_kafun_moe-archives.R`... 全国、2003年から2018年
