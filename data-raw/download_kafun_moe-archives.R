#################################
# 
# 過去データ(2003~2018)
#################################
library(rvest)
library(stringr)
library(purrr)
library(ensurer)
library(conflicted)

if (rlang::is_false(dir.exists(here::here("data-raw/moe"))))
  dir.create(here::here("data-raw/moe"))

site_url <- "http://kafun.taiki.go.jp/"

x <- 
  str_c(site_url, "library.html") %>% 
  read_html()

data_urls <- 
  x %>% 
  html_nodes(css = '#Table5 > tr > td > a') %>% 
  html_attr("href") %>% 
  str_c(site_url, .) %>% 
  ensure_that(length(.) == 92)

download_zip <- 
  slowly(~ .x %>% 
           curl::curl_download(destfile = here::here("data-raw/moe", basename(.))), 
         rate = rate_delay(pause = 3), 
         quiet = FALSE)

data_urls %>% 
  purrr::walk(download_zip)

# fs::dir_ls(here::here("data-raw/moe"), 
#            regexp = ".zip$") %>% 
#   walk(~ unzip(zipfile = .x, exdir = here::here("data-raw/moe")))

# Illegal byte sequenceでunzipが成功しない場合は下記を行う
# unarコマンドのインストールが必要。macOSなら `brew install unar`
zips <-
  fs::dir_ls(here::here("data-raw/moe"), 
             regexp = ".zip$")

zips %>% 
  walk(function(x) {
    system(glue::glue("unar -o {exdir} {zip}", exdir = here::here("data-raw/moe"), zip = x))
  })

zips %>% 
  unlink()


# 列番号,項目,備考
# 1,測定局コード,
# 2,アメダス測定局コード,
# 3,年月日,
# 4,時,
# 5,測定局名,
# 6,測定局種別,1：都市部、2：山間部、0：区分なし
# 7,都道府県コード,01〜47
# 8,都道府県名,
# 9,市区町村コード,5桁
# 10,市区町村名,
# 11,花粉飛散数[個/m3],
# 12,風向,0：静穏、1：北北東、2：北東、3：東北東、4：東、5：東南東、6：南東、7：南南東、8：南、9：南南西、10：南西、11：西南西、12：西、13：西北西、14：北西、15：北北西、16：北
# 13,風速[m/s],
# 14,気温[℃],
# 15,降水量[mm],
# 16,レーダー降水量[mm],

data_archives <- 
  fs::dir_ls(here::here("data-raw/moe"), recursive = TRUE, regexp = "花粉データ.+(xls|xlsx)$")

library(dplyr)
vars <- 
  data_archives[6] %>% 
  readxl::read_excel(n_max = 1, skip = 1) %>% 
  names()

# 2005はnames()

d <- 
  data_archives[1] %>% 
  readxl::read_excel() %>% 
  tidyr::gather("station", "value", -1) %>% 
  set_names(c("datetime", "station", "value"))

d <- 
  d %>% 
  mutate(datetime = str_c("2005年", datetime, "00分00秒") %>% 
           lubridate::as_datetime())

library(ggplot2)
ggplot(d, aes(datetime, value, group = station, color = station)) +
  geom_line()

lubridate::as_date("2005年02月01日")

# 観測地点 --------------------------------------------------------------------
extract_station_list <- function(table_id, region) {
  
  x %>% 
    html_nodes(css = table_id) %>% 
    html_table(header = TRUE) %>% 
    purrr::map2(
      .x = .,
      .y = region,
      .f = ~ tidyrup_station_list(.x, .y))
}

extract_station_list("#Table9", list(c("北海道", "関東")))

x %>% 
  html_nodes(css = '#Table9') %>% 
  html_table(header = TRUE) %>% 
  purrr::map2(
    .x = .,
    .y = c("北海道", "関東"),
    .f = ~ tidyrup_station_list(.x, .y))
x %>% 
  html_nodes(css = '#Table10') %>% 
  html_table(header = TRUE) %>% 
  purrr::map2(
    .x = .,
    .y = c("東北", "中部"),
    .f = ~ tidyrup_station_list(.x, .y)
  )

jpndistrict::jpnprefs %>% 
  distinct(region, .keep_all = T)

x %>% 
  html_nodes(css = '#Table11') %>% 
  html_table(header = TRUE) %>% 
  purrr::map2(
    .x = .,
    .y = c("近畿"),
    .f = ~ tidyrup_station_list(.x, .y))
x %>% 
  html_nodes(css = '#Table12') %>% 
  html_table(header = TRUE) %>% 
  purrr::map2(
    .x = .,
    .y = list(c("中国", "四国")),
    .f = ~ tidyrup_station_list(.x, .y))
x %>% 
  html_nodes(css = '#Table13') %>% 
  html_table(header = TRUE) %>% 
  purrr::map2(
    .x = .,
    .y = c("九州"),
    .f = ~ tidyrup_station_list(.x, .y))
x %>% 
  html_nodes(css = '#Table14') %>% 
  html_table(header = TRUE) %>% 
  purrr::map2(
    .x = .,
    .y = c("九州"),
    .f = ~ tidyrup_station_list(.x, .y))

tidyrup_station_list <- function(df, region = NULL) {
  
  region <- rlang::syms(region)

  df_gather <- 
    df %>% 
    rename(pref = `都道府県`,
           station = `設置場所`,
           address = `所在地`,
           amedas_station = `最寄のアメダス測定局`,
           note = `備考`) %>% 
    tidyr::gather("year", "status", -pref, -station, -address, -amedas_station, -note) %>% 
    tibble::as_tibble()
    
  df_gather %>% 
    mutate(status = if_else(str_detect(status, "\u25cb"), TRUE, FALSE)) %>% 
    fuzzyjoin::stringdist_left_join(jpndistrict::jpnprefs %>%
                                      dplyr::filter(region %in% c(!! region)) %>% 
                                      dplyr::select(prefecture),
                                    by = c("pref" = "prefecture"), 
                                    max_dist = 1,
                                    method = "osa") %>% 
    select(prefecture, station, address, year, status, everything(), -pref) %>% 
    mutate_at(vars(station, address), stringi::stri_trans_nfkc) %>% 
    assertr::verify(nrow(.) == nrow(df_gather))
}

tidyrup_station_list(dd[[1]], region = "近畿")

ddd <- 
  dd[[1]] %>% 
  rename(pref = `都道府県`,
         station = `設置場所`,
         address = `所在地`) %>% 
  tidyr::gather("year", "status", -pref, -station, -address) %>% 
  mutate(status = if_else(str_detect(status, "\u25cb"), TRUE, FALSE)) %>% 
  fuzzyjoin::stringdist_left_join(jpndistrict::jpnprefs %>%
                                    dplyr::filter(region %in% c("近畿")) %>% 
                                    select(prefecture),
                        by = c("pref" = "prefecture"), 
                        max_dist = 1,
                        method = "osa") %>% 
  select(prefecture, everything(), -pref)

