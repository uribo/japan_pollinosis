#################################
# 環境省 環境省花粉観測システム
# 過去データ(2003~2018)
# 2008年以降、北海道は6月末までデータがある
#################################
library(rvest)
library(stringr)
library(purrr)
library(ensurer)
library(dplyr)
library(conflicted)
conflict_prefer("filter", winner = "dplyr")

if (rlang::is_false(dir.exists(here::here("data-raw/moe"))))
  dir.create(here::here("data-raw/moe"))

data_archives <- 
  fs::dir_ls(here::here("data-raw/moe"), 
             recursive = TRUE, 
             regexp = "花粉データ.+.(xls|xlsx)$")

if (length(data_archives) < 92) {
  site_url <- "http://kafun.taiki.go.jp/"
  
  data_urls <- 
    str_c(site_url, "library.html") %>% 
    read_html(encoding = "cp932") %>% 
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
  # unarコマンドのインストールが必要。
  # - ubuntuなら `apt-get install unar`
  # - macOSなら `brew install unar`
  zips <-
    fs::dir_ls(here::here("data-raw/moe"), 
               regexp = ".zip$") %>% 
    ensure_that(length(.) == 92)
  
  zips %>% 
    walk(function(x) {
      system(glue::glue("unar -o {exdir} {zip}", exdir = here::here("data-raw/moe"), zip = x))
      
      unlink(x)
    })
  
  data_archives <- 
    fs::dir_ls(here::here("data-raw/moe"), 
               recursive = TRUE, 
               regexp = "花粉データ.+.(xls|xlsx)$")
  
}

# 2003 --------------------------------------------------------------------
parse_xls_data <- function(input, year = 2003) {
  
  if (year %in% seq.int(2003, 2005, by = 1)) {
    
    d <- 
      readxl::read_excel(input) %>% 
      rename(datetime = 1) %>% 
      mutate(datetime = str_c(year, "年", datetime, "00分00秒") %>% 
               lubridate::as_datetime(tz = "Asia/Tokyo"))
    
  } else if (year %in% seq.int(2006, 2018, by = 1)) {
    d <- readxl::read_excel(input, 
                       skip = 1, 
                       sheet = 1, 
                       na = c("-9998", "-9997", "-9996")) %>% 
      dplyr::select(-seq(ncol(.) - 1, ncol(.))) %>% 
      dplyr::mutate_all(as.numeric) %>% 
      mutate(datetime = lubridate::make_datetime(year = `年`, month = `月`, day = `日`, hour = `時`, 
                                                 tz = "Asia/Tokyo")) %>% 
      select(datetime, 5:ncol(.))
  } 
  
  d %>% 
    tidyr::gather(station, value, -datetime)
  
}

tgt_files <- 
  seq.int(2003, 2018) %>%
  as.character() %>% 
  purrr::map(~ str_subset(data_archives, .x)) %>% 
  purrr::map2(
    .x = .,
    .y = c(1, 2, 3, 4, 5, 7,
           7, 7, 7, 7, 7, 7,
           7, 7, 7, 7),
    .f = ~ .x %>% ensure_that(length(.) == .y)) %>% 
  purrr::set_names(str_c("archives_", seq.int(2003, 2018)))

df_pollen_archives_moe <- 
  purrr::map2(
  .x = tgt_files %>% 
    unlist() %>% 
    c(),
  .y = purrr::map2(.x = names(tgt_files) %>% 
                     readr::parse_number(),
                   .y = tgt_files %>% 
                     purrr::map(length),
                   .f = ~ rep(.x, each = .y)) %>% 
    purrr::reduce(c),
  .f = ~ parse_xls_data(.x, year = .y))

df_pollen_archives_moe %>% 
  readr::write_rds(here::here("data/japan_archives.rds"), compress = "xz")

library(ggplot2)
ggplot(df_pollen_archives_moe[[1]], aes(datetime, value, group = station, color = station)) +
  geom_line()

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

