############################
# 測定局の住所を位置参照情報データによってジオコーディング
# 街区レベル(pos_level = 0)までは必要ない。大字・町丁目レベル(pos_level = 1)で十分
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
  df_moe_stations_location <- 
    df_moe_stations_location %>% 
    mutate(
      address = modified_address(city, address, "札幌市北区", "北19条西12", "北十九条西十二丁目"),
      address = modified_address(city, address, "函館市", "美原4-", "美原四丁目"),
      address = modified_address(city, address, "旭川市", "永山6条19", "永山六条十九丁目"),
      address = modified_address(city, address, "帯広市", "東3条南3", "東三条南三丁目"),
      address = modified_address(city, address, "水戸市", "石川1", "石川一丁目"),
      address = modified_address(city, address, "日立市", "神峰町2", "神峰町二丁目"),
      address = modified_address(city, address, "つくば市", "小野川16-", "小野川"),
      address = modified_address(city, address, "宇都宮市", "中央1", "中央一丁目"),
      address = modified_address(city, address, "大田原市", "中央1", "中央一丁目"),
      address = modified_address(city, address, "日光市", "今市本町1", "今市本町"),
      address = modified_address(city, address, "前橋市", "上沖町378", "上沖町"),
      address = modified_address(city, address, "館林市", "大街道1", "大街道一丁目"),
      address = modified_address(city, address, "さいたま市浦和区", "常盤6", "常盤六丁目"),
      address = modified_address(city, address, "熊谷市", "箱田1", "箱田一丁目"),
      address = modified_address(city, address, "飯能市", "双柳1", "大字双柳"),
      address = modified_address(city, address, "船橋市", "三山2", "三山二丁目"),
      address = modified_address(city, address, "市原市", "岩崎西1", "岩崎西一丁目"),
      address = modified_address(city, address, "成田市", "加良部3", "加良部三丁目"),
      address = modified_address(city, address, "君津市", "糠田55", "糠田"),
      address = modified_address(city, address, "小平市", "花小金井1", "花小金井一丁目"),
      address = modified_address(city, address, "新宿区", "新宿5", "新宿五丁目"),
      address = modified_address(city, address, "横浜市中区", "日本大通5", "日本大通"),
      # address = if_else(city == "川崎市川崎区" & stringr::str_detect(address, "殿町"),
      #                  "殿町三丁目",
      #                  address),
      address = modified_address(city, address, "川崎市川崎区", "殿町3", "殿町三丁目"),
      address = modified_address(city, address, "平塚市", "四之宮1", "四之宮一丁目"),
      address = modified_address(city, address, "青森市", "卸町1", "卸町"),
      address = modified_address(city, address, "弘前市", "文京町", "大字文京町"),
      address = modified_address(city, address, "盛岡市", "飯岡新田", "飯岡新田"),
      address = modified_address(city, address, "大船渡市", "猪川町", "猪川町"),
      address = modified_address(city, address, "仙台市青葉区", "星陵町", "星陵町"),
      address = modified_address(city, address, "石巻市", "あゆみ野5", "恵み野五丁目"),
      address = modified_address(city, address, "秋田市", "千秋久保田町", "千秋久保田町"),
      address = modified_address(city, address, "横手市", "旭川1", "旭川一丁目"),
      address = modified_address(city, address, "山形市", "十日町1", "十日町一丁目"),
      address = modified_address(city, address, "米沢市", "金池3", "金池三丁目"),
      address = modified_address(city, address, "東田川郡三川町", "大字横山", "大字横山"),
      address = modified_address(city, address, "福島市", "方木田", "方木田"),
      address = modified_address(city, address, "いわき市", "工業団地", "工業団地"),
      address = modified_address(city, address, "新潟市西区", "曽和", "曽和"),
      address = modified_address(city, address, "長岡市", "川崎町", "川崎町"),
      address = modified_address(city, address, "上越市", "春日山町3", "春日山町三丁目"),
      address = modified_address(city, address, "富山市", "新総曲輪", "新総曲輪"),
      address = modified_address(city, address, "魚津市", "新宿", "新宿"),
      address = modified_address(city, address, "金沢市", "宝町", "宝町"),
      address = modified_address(city, address, "七尾市", "本府中町", "本府中町"),
      address = modified_address(city, address, "福井市", "豊島2", "豊島二丁目"),
      address = modified_address(city, address, "敦賀市", "開町", "開町"),
      address = modified_address(city, address, "甲府市", "富士見1", "富士見一丁目"),
      address = modified_address(city, address, "南巨摩郡身延町", "梅平", "梅平"),
      address = modified_address(city, address, "長野市", "安茂里米村", "大字安茂里"),
      address = modified_address(city, address, "飯田市", "追手町2", "追手町二丁目"),
      address = modified_address(city, address, "松本市", "島立", "大字島立"),
      address = modified_address(city, address, "大垣市", "南頬町4", "南頬町四丁目"),
      address = modified_address(city, address, "郡上市", "八幡町初音", "八幡町初音"),
      address = modified_address(city, address, "静岡市葵区", "追手町", "追手町"),
      address = modified_address(city, address, "沼津市", "高島本町", "高島本町"),
      address = modified_address(city, address, "伊東市", "大原2", "大原二丁目"),
      address = modified_address(city, address, "名古屋市北区", "辻町", "辻町"),
      address = modified_address(city, address, "豊橋市", "八町通5", "八町通五丁目"),
      address = modified_address(city, address, "四日市市", "日永", "大字日永"),
      address = modified_address(city, address, "津市", "広明町", "広明町"),
      address = modified_address(city, address, "彦根市", "城町2丁目", "城町二丁目"),
      address = modified_address(city, address, "大津市", "柳が崎", "柳が崎"),
      address = modified_address(city, address, "高島市", "今津町今津", "今津町今津"),
      address = modified_address(city, address, "京都市上京区", "河原町広小路梶井町", "梶井町"),
      address = modified_address(city, address, "京都市右京区", "京北周山町", "京北周山町"),
      address = modified_address(city, address, "舞鶴市", "字南田辺", "字南田辺"),
      address = modified_address(city, address, "大阪市中央区", "大手前4", "大手前四丁目"),
      address = modified_address(city, address, "豊中市", "中桜塚3", "中桜塚三丁目"),
      address = modified_address(city, address, "泉大津市", "東雲町", "東雲町"),
      address = modified_address(city, address, "加古川市", "神野町神野", "神野町神野"),
      address = modified_address(city, address, "西宮市", "北山町", "北山町"),
      address = modified_address(city, address, "篠山市", "群家", "郡家"),
      address = modified_address(city, address, "神戸市須磨区", "行平町3", "行平町三丁目"),
      address = modified_address(city, address, "奈良市", "柏木町", "柏木町"),
      address = modified_address(city, address, "吉野郡下市町", "新庄", "大字新住"),
      address = modified_address(city, address, "橿原市", "常盤町", "常盤町"),
      address = modified_address(city, address, "和歌山市", "男野芝丁", "男野芝丁"),
      address = modified_address(city, address, "田辺市", "朝日ヶ丘", "朝日ヶ丘"),
      address = modified_address(city, address, "新宮市", "緑ヶ丘2", "緑ケ丘二丁目"),
      address = modified_address(city, address, "鳥取市", "西町1", "西町一丁目"),
      address = modified_address(city, address, "倉吉市", "東巌城町", "東巌城町"),
      address = modified_address(city, address, "松江市", "西浜佐陀町", "西浜佐陀町"),
      address = modified_address(city, address, "浜田市", "片庭町", "片庭町"),
      address = modified_address(city, address, "笠岡市", "六番町", "六番町"),
      address = modified_address(city, address, "真庭市", "勝山", "勝山"),
      address = modified_address(city, address, "岡山市北区", "鹿田町2", "鹿田町二丁目"),
      address = modified_address(city, address, "広島市南区", "皆実町1", "皆実町一丁目"),
      address = modified_address(city, address, "三原市", "円一町2", "円一町二丁目"),
      address = modified_address(city, address, "光市", "岩田", "大字岩田"),
      address = modified_address(city, address, "宇部市", "南小串1", "南小串一丁目"),
      address = modified_address(city, address, "山口市", "葵2丁目", "葵二丁目"),
      address = modified_address(city, address, "徳島市", "新蔵町3丁目", "新蔵町三丁目"),
      address = modified_address(city, address, "那賀郡那賀町", "吉野", "吉野"),
      address = modified_address(city, address, "高松市", "番町4", "番町四丁目"),
      address = modified_address(city, address, "善通寺市", "文京町2", "文京町二丁目"),
      address = modified_address(city, address, "新居浜市", "一宮町1", "一宮町一丁目"),
      address = modified_address(city, address, "松山市", "樽味3", "樽味三丁目"),
      address = modified_address(city, address, "宇和島市", "曙町", "曙町"),
      address = modified_address(city, address, "高知市", "丸ノ内2", "丸ノ内二丁目"),
      address = modified_address(city, address, "四万十市", "中村山手通", "中村山手通"),
      address = modified_address(city, address, "北九州市小倉北区", "中島1", "中島一丁目"),
      address = modified_address(city, address, "久留米市", "津福本町", "津福本町"),
      address = modified_address(city, address, "田川市", "糒", "大字糒"),
      address = modified_address(city, address, "佐賀市", "鍋島町八戸溝", "鍋島町大字八戸溝"),
      address = modified_address(city, address, "唐津市", "西城内", "西城内"),
      address = modified_address(city, address, "武雄市", "武雄町大字昭和", "武雄町大字昭和"),
      address = modified_address(city, address, "長崎市", "坂本1", "坂本一丁目"),
      address = modified_address(city, address, "諫早市", "永昌東町", "永昌東町"),
      address = modified_address(city, address, "佐世保市", "木場田町3", "木場田町"),
      address = modified_address(city, address, "熊本市中央区", "本荘5", "本荘五丁目"),
      address = modified_address(city, address, "水俣市", "浜", "浜"),
      # address = modified_address(city, address, "阿蘇郡高森町", "高森", "大字高森"),
      address = modified_address(city, address, "佐伯市", "長島町1", "長島町一丁目"),
      address = modified_address(city, address, "由布市", "挾間町医大ケ丘1", "挾間町医大ケ丘一丁目"),
      address = modified_address(city, address, "日田市", "大字有田", "大字有田"),
      address = modified_address(city, address, "延岡市", "大貫町1", "大貫町一丁目"),
      address = modified_address(city, address, "宮崎市", "橘通東2", "橘通東二丁目"),
      address = modified_address(city, address, "鹿児島市", "城南町", "城南町"),
      address = modified_address(city, address, "伊佐市", "大口里", "大口里"),
      address = modified_address(city, address, "阿南市", "領家町野神", "領家町"),
      address = modified_address(city, address, "鹿屋市", "打馬2", "打馬二丁目"))
  
  df_moe_stations_location <- 
    df_moe_stations_location %>% 
    select(prefecture, station, address) %>% 
    mutate(address_elem = purrr::pmap(.,
                                      ~ zipangu::separate_address(..3))) %>% 
    tidyr::hoist(address_elem,
                 prefecture = "prefecture",
                 city = "city",
                 street = "street")
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
