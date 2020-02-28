############################
# 測定局の住所を位置参照情報データによってジオコーディング
# 街区レベル(pos_level = 0)までは必要ない。大字・町丁目レベル(pos_level = 1)で十分
# 整備状況的にも大字・町丁目レベルに留めておくのが良さげ
# いくつかの住所は丸め込み。
# (施設名称で検索した方が良いのでは感もあるが、結局測定機械のある座標には落とせないのでまあいいか)
############################
library(dplyr)
if (file.exists(here::here("data/moe_stations_location.csv")) == FALSE) {
  source(here::here("data-raw/collect_moe_stations.R")) # df_moe_stations  
  source("https://raw.githubusercontent.com/uribo/jp-address/master/R/modified_address.R")
  library(lubridate)
  library(assertr)
  library(kuniumi)
  library(stringr)

  # ファイルサイズが大きくなる(rds xz format...78.6MB)ので保存はしない
  # ~ 5 min
  df_isj_a <- 
    fs::dir_ls("~/Documents/projects2019/jp-address/data-raw/位置参照情報/街区レベル/h30/",
               recurse = TRUE,
               regexp = "[0-9]{5}_2018.csv") %>% 
    purrr::map_dfr(
      ~ read_isj(.x, return_class = "data.table"))
  df_isj_a <- 
    df_isj_a[, list(prefecture, city, street_lv1, street_lv1b, street_lv3, cs_num, latitude, longitude, flag_represent)]
  df_isj_a[city == "真庭市" & stringr::str_detect(street_lv1, "勝山")][["street_lv3"]] %>% 
    stringr::str_subset("591")
  # ~ 1 min
  df_isj_b <- 
    fs::dir_ls("~/Documents/projects2019/jp-address/data-raw/位置参照情報/大字・町丁目レベル/h30/",
               regexp = ".csv$", 
               recurse = TRUE) %>% 
    purrr::map_dfr(
      ~ read_isj(.x, return_class = "data.table"))
  df_moe_stations_location <- 
    df_moe_stations %>% 
    distinct(prefecture, station, address) %>% 
    mutate(address = paste0(prefecture, address)) %>% 
    mutate(address = recode(
      address,
      `神奈川県川崎市川崎区殿町3` = "神奈川県川崎市川崎区殿町三丁目",
      `熊本県阿蘇郡高森町高森3219` = "熊本県阿蘇郡高森町大字高森"
    )) %>% 
    mutate(address_elem = purrr::pmap(.,
                                      ~ zipangu::separate_address(..3))) %>% 
    tidyr::hoist(address_elem,
                 prefecture = "prefecture",
                 city = "city",
                 street = "street")
  df_moe_stations_location %>% 
    filter(station == "北海道渡島総合振興局")
  df_moe_stations_location %>% 
    slice(1L)
  df_isj_b %>% 
    filter(city == "札幌市北区", str_detect(street_lv1, "北十九条西十二丁目"))
  df_moe_stations_location <- 
    df_moe_stations_location %>% 
    mutate(
      address = modified_address(city, address, target_city = "札幌市北区", fix_adds = "北19条西12", replace = "北十九条西十二丁目"),
      address = modified_address(city, address, "函館市", "美原4-6", "美原四丁目-6"),
      address = modified_address(city, address, "旭川市", "永山6条19", "永山六条十九丁目"),
      address = modified_address(city, address, "帯広市", "東3条南3", "東三条南三丁目"),
      address = modified_address(city, address, "水戸市", "石川1-4043", "石川一丁目-4043"),
      address = modified_address(city, address, "日立市", "神峰町2-4", "神峰町二丁目-4"),
      address = modified_address(city, address, "つくば市", "小野川16-2", "小野川-16"),
      address = modified_address(city, address, "宇都宮市", "中央1-1", "中央一丁目-1"),
      address = modified_address(city, address, "大田原市", "中央1-9", "中央一丁目-9"),
      address = modified_address(city, address, "日光市", "今市本町1", "今市本町-1"),
      # address = modified_address(city, address, "前橋市", "上沖町378", "上沖町378"),
      address = modified_address(city, address, "館林市", "大街道1-2", "大街道一丁目-2"),
      address = modified_address(city, address, "さいたま市浦和区", "常盤6-4", "常盤六丁目-4"),
      address = modified_address(city, address, "熊谷市", "箱田1-2", "箱田一丁目-2"),
      address = modified_address(city, address, "飯能市", "双柳1", "大字双柳-1"),
      address = modified_address(city, address, "船橋市", "三山2-2", "三山二丁目-2"),
      address = modified_address(city, address, "市原市", "岩崎西1-8", "岩崎西一丁目-8"),
      address = modified_address(city, address, "成田市", "加良部3-3", "加良部三丁目-3"),
      address = modified_address(city, address, "君津市", "糠田55", "糠田"),
      address = modified_address(city, address, "小平市", "花小金井1-31", "花小金井一丁目-31"),
      address = modified_address(city, address, "新宿区", "新宿5丁目18", "新宿五丁目-18"),
      address = modified_address(city, address, "横浜市中区", "日本大通5", "日本大通-5"),
      # address = if_else(city == "川崎市川崎区" & stringr::str_detect(address, "殿町"),
      #                  "殿町三丁目",
      #                  address),
      address = modified_address(city, address, "川崎市川崎区", "殿町3", "殿町三丁目"),
      address = modified_address(city, address, "平塚市", "四之宮1-3", "四之宮一丁目-3"),
      address = modified_address(city, address, "青森市", "卸町1", "卸町1"),
      address = modified_address(city, address, "弘前市", "文京町1", "大字文京町-1"),
      address = modified_address(city, address, "盛岡市", "飯岡新田", "飯岡新田"),
      address = modified_address(city, address, "大船渡市", "猪川町字前田6", "猪川町前田-6"),
      address = modified_address(city, address, "仙台市青葉区", "星陵町2", "星陵町-2"),
      address = modified_address(city, address, "石巻市", "あゆみ野5", "恵み野五丁目"),
      address = modified_address(city, address, "秋田市", "千秋久保田町6", "千秋久保田町-6"),
      address = modified_address(city, address, "横手市", "旭川1-3", "旭川一丁目-3"),
      address = modified_address(city, address, "山形市", "十日町1-6", "十日町一丁目-6"),
      address = modified_address(city, address, "米沢市", "金池3-1", "金池三丁目-1"),
      address = modified_address(city, address, "東田川郡三川町", "大字横山", "大字横山"),
      address = modified_address(city, address, "福島市", "方木田字水戸内16-6", "方木田水戸内-16"),
      address = modified_address(city, address, "いわき市", "工業団地", "好間工業団地-4"),
      address = modified_address(city, address, "新潟市西区", "曽和314", "曽和-314"),
      address = modified_address(city, address, "長岡市", "川崎町", "川崎町"),
      address = modified_address(city, address, "上越市", "春日山町3", "春日山町三丁目"),
      address = modified_address(city, address, "富山市", "新総曲輪1", "新総曲輪-1"),
      address = modified_address(city, address, "魚津市", "新宿10", "新宿-10"),
      address = modified_address(city, address, "金沢市", "宝町13", "宝町-13"),
      address = modified_address(city, address, "七尾市", "本府中町ソ-27", "本府中町-ソ-27"),
      address = modified_address(city, address, "福井市", "豊島2-5", "豊島二丁目-5"),
      address = modified_address(city, address, "敦賀市", "開町6", "開町-6"),
      address = modified_address(city, address, "甲府市", "富士見1-7", "富士見一丁目-7"),
      address = modified_address(city, address, "南巨摩郡身延町", "梅平2483", "梅平-2483"),
      address = modified_address(city, address, "長野市", "安茂里米村1978", "大字安茂里-1978"),
      address = modified_address(city, address, "飯田市", "追手町2-678", "追手町二丁目-678"),
      address = modified_address(city, address, "松本市", "島立", "大字島立-1020"),
      address = modified_address(city, address, "大垣市", "南頬町4-86", "南頬町四丁目-86"),
      address = modified_address(city, address, "郡上市", "八幡町初音1727", "八幡町初音-1727"),
      address = modified_address(city, address, "静岡市葵区", "追手町", "追手町-9"),
      address = modified_address(city, address, "沼津市", "高島本町1", "高島本町-1"),
      address = modified_address(city, address, "伊東市", "大原2-1", "大原二丁目-1"),
      address = modified_address(city, address, "名古屋市北区", "辻町7", "辻町-7"),
      address = modified_address(city, address, "豊橋市", "八町通5-4", "八町通五丁目-4"),
      address = modified_address(city, address, "四日市市", "日永5450", "大字日永-5450"),
      address = modified_address(city, address, "津市", "広明町13", "広明町-13"),
      address = modified_address(city, address, "彦根市", "城町2丁目-5", "城町二丁目-5"),
      address = modified_address(city, address, "大津市", "柳が崎5", "柳が崎-5"),
      address = modified_address(city, address, "高島市", "今津町今津1758", "今津町今津-1758"),
      address = modified_address(city, address, "京都市上京区", "河原町広小路梶井町465", "梶井町-465"),
      address = modified_address(city, address, "京都市右京区", "京北周山町", "京北周山町"),
      address = modified_address(city, address, "舞鶴市", "字南田辺1", "字南田辺-1"),
      address = modified_address(city, address, "大阪市中央区", "大手前4-1", "大手前四丁目-1"),
      address = modified_address(city, address, "豊中市", "中桜塚3-1", "中桜塚三丁目-1"),
      address = modified_address(city, address, "泉大津市", "東雲町9", "東雲町-9"),
      address = modified_address(city, address, "加古川市", "神野町神野242", "神野町神野-242"),
      address = modified_address(city, address, "西宮市", "北山町1", "北山町-1"),
      address = modified_address(city, address, "篠山市", "群家451", "郡家-451"),
      address = modified_address(city, address, "神戸市須磨区", "行平町3-1", "行平町三丁目-1"),
      address = modified_address(city, address, "奈良市", "柏木町129", "柏木町-129"),
      address = modified_address(city, address, "吉野郡下市町", "新庄15", "大字新住-15"),
      address = modified_address(city, address, "橿原市", "常盤町616", "常盤町-616"),
      address = modified_address(city, address, "和歌山市", "男野芝丁4", "男野芝丁-4"),
      address = modified_address(city, address, "田辺市", "朝日ヶ丘23", "朝日ヶ丘-23"),
      address = modified_address(city, address, "新宮市", "緑ヶ丘2-4", "緑ケ丘二丁目-4"),
      address = modified_address(city, address, "鳥取市", "西町1-401", "西町一丁目-401"),
      address = modified_address(city, address, "倉吉市", "東巌城町2", "東巌城町-2"),
      address = modified_address(city, address, "松江市", "西浜佐陀町582", "西浜佐陀町-582"),
      address = modified_address(city, address, "浜田市", "片庭町254", "片庭町-254"),
      address = modified_address(city, address, "笠岡市", "六番町-2", "六番町-2"),
      address = modified_address(city, address, "真庭市", "勝山591", "勝山-591"),
      address = modified_address(city, address, "岡山市北区", "鹿田町2-5", "鹿田町二丁目-5"),
      address = modified_address(city, address, "広島市南区", "皆実町1-6", "皆実町一丁目-6"),
      address = modified_address(city, address, "三原市", "円一町2-4", "円一町二丁目-4"),
      address = modified_address(city, address, "光市", "岩田974", "大字岩田-974"),
      address = modified_address(city, address, "宇部市", "南小串1", "南小串一丁目"),
      address = modified_address(city, address, "山口市", "葵2丁目-5", "葵二丁目-5"),
      address = modified_address(city, address, "徳島市", "新蔵町3丁目-80", "新蔵町三丁目-80"),
      address = modified_address(city, address, "高松市", "番町4-1", "番町四丁目-1"),
      address = modified_address(city, address, "善通寺市", "文京町2-1", "文京町二丁目-1"),
      address = modified_address(city, address, "新居浜市", "一宮町1-5", "一宮町一丁目-5"),
      address = modified_address(city, address, "松山市", "樽味3-5", "樽味三丁目-5"),
      address = modified_address(city, address, "宇和島市", "曙町1", "曙町-1"),
      address = modified_address(city, address, "高知市", "丸ノ内2-4", "丸ノ内二丁目-4"),
      address = modified_address(city, address, "四万十市", "中村山手通19", "中村山手通-19"),
     # address = modified_address(city, address, "那賀郡那賀町", "吉野", "吉野"),
      address = modified_address(city, address, "北九州市小倉北区", "中島1-19", "中島一丁目-19"),
      address = modified_address(city, address, "久留米市", "津福本町2241", "津福本町-2241"),
      address = modified_address(city, address, "田川市", "糒1700", "大字糒1-700"),
      address = modified_address(city, address, "佐賀市", "鍋島町八戸溝119", "鍋島町大字八戸溝-119"),
      address = modified_address(city, address, "唐津市", "西城内1", "西城内-1"),
      address = modified_address(city, address, "武雄市", "武雄町大字昭和265", "武雄町大字昭和-265"),
      address = modified_address(city, address, "長崎市", "坂本1-7", "坂本一丁目-7"),
      address = modified_address(city, address, "諫早市", "永昌東町24", "永昌東町-24"),
      address = modified_address(city, address, "佐世保市", "木場田町3", "木場田町-3"),
      address = modified_address(city, address, "熊本市中央区", "本荘5-15", "本荘五丁目-15"),
      address = modified_address(city, address, "水俣市", "浜4058", "浜-4058"),
      # address = modified_address(city, address, "阿蘇郡高森町", "高森", "大字高森"),
      address = modified_address(city, address, "佐伯市", "長島町1-2", "長島町一丁目-2"),
      address = modified_address(city, address, "由布市", "挾間町医大ケ丘1-1", "挾間町医大ケ丘一丁目-1"),
      address = modified_address(city, address, "日田市", "大字有田", "大字有田"),
      address = modified_address(city, address, "延岡市", "大貫町1-2840", "大貫町一丁目-2840"),
      address = modified_address(city, address, "宮崎市", "橘通東2-10", "橘通東二丁目-10"),
      address = modified_address(city, address, "鹿児島市", "城南町18", "城南町-18"),
      address = modified_address(city, address, "伊佐市", "大口里", "大口里-53"),
      address = modified_address(city, address, "鹿屋市", "打馬2-16", "打馬二丁目-16"),
      address = modified_address(city, address, "阿南市", "領家町野神", "領家町"))
  df_moe_stations_location <- 
    df_moe_stations_location %>% 
    select(prefecture, station, address) %>% 
    mutate(address_elem = purrr::pmap(.,
                                      ~ zipangu::separate_address(..3))) %>% 
    tidyr::hoist(address_elem,
                 prefecture = "prefecture",
                 city = "city",
                 street = "street")
  dd %>% 
    #slice(2L) %>% 
    tidyr::extract(col = street, into = c("street_lv1", "street_lv3"), regex = "(.+丁目)(.+)", remove = FALSE) %>%
    mutate(street_lv1 = if_else(is.na(street_lv1), street, street_lv1)) %>% 
    left_join(df_isj_a, by = c("prefecture", "city", "street_lv1", "street_lv3")) %>% 
    filter(!is.na(longitude))
  
  df_isj_a[prefecture == "大分県" & city == "日田市" & stringr::str_detect(street_lv1, "大字有田")]
  dd %>% 
    left_join(df_isj_a, by = c("prefecture", "city", "street" = "street_lv1"))
  # df_isj_a[prefecture == "北海道" & city == "函館市" & stringr::str_detect(street_lv1, "美原四丁目") & street_lv3 == "6"] %>% 
  #   as.data.frame() %>% 
  #   sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  #   mapview::mapview()
  df_moe_stations_location %>% 
    pull(address) %>% 
    tail()
  invisible(
    df_moe_stations_location %>% 
      anti_join(df_isj_b %>% 
                  select(-street_levels, -street_lv1_code),
                by = c("prefecture", "city", "street" = "street_lv1")) %>% 
      verify(nrow(.) == 0L))
  df_moe_stations_location <- 
    df_moe_stations_location %>%
    left_join(df_isj_b %>%
                select(-street_levels, -street_lv1_code),
              by = c("prefecture", "city", "street" = "street_lv1")) %>% 
    select(-prefecture, -city, -street) %>% 
    verify(dim(.) == c(120, 5))
  df_moe_stations_location %>%
    readr::write_csv(here::here("data/moe_stations_location.csv"))
} else {
  df_moe_stations_location <- 
    readr::read_csv(here::here("data/moe_stations_location.csv"),
                    col_types = c("cccdd"))
}

df_moe_stations_location %>%
  sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  mapview::mapview()
