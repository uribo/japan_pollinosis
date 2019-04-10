###############################
# docker起動状態で行う
# 以下のコードはローカルで実行
##############################
if (rlang::is_false(file.exists(here::here("data/japan_archives2019.rds")))) {

  library(RSelenium)
  library(purrr)
  library(dplyr)
  library(assertr)
  library(ensurer)
  library(rvest)
  library(conflicted)
  conflict_prefer("pluck", winner = "purrr")
  
  rD <- rsDriver(verbose = FALSE, 
                 port = 4445L)
  remDr <- rD[["client"]]
  
  # データ収集期間・地域を指定 ----------------------------------------------------------------------
  # 20190201 (1時) ~ 20190331 (24時)
  # 地域によって異なるので注意
  remDr$navigate("http://kafun.taiki.go.jp/DownLoad1.aspx")
  
  webElem <- 
    remDr$findElement("css selector",
                      "#ddlStartYear")
  
  webElem$getElementText() %>%
    unlist() %>%
    ensure(stringr::str_detect(., "2019"))
  
  webElem <-
    remDr$findElement("css selector",
                      "#ddlStartMonth > option:nth-child(2)")
  # webElem$getElementText()
  webElem$clickElement()
  
  webElem <-
    remDr$findElement("css selector",
                      "#ddlStartDay > option:nth-child(1)")
  webElem$clickElement()
  
  webElem <-
    remDr$findElement("css selector",
                      "#ddlStartHour > option:nth-child(1)")
  webElem$clickElement()
  
  webElem <-
    remDr$findElement("css selector",
                      "#ddlEndYear")
  webElem$getElementText()
  
  webElem <-
    remDr$findElement("css selector",
                      "#ddlEndMonth > option:nth-child(3)")
  webElem$clickElement()
  
  webElem <-
    remDr$findElement("css selector",
                      "#ddlEndDay > option:nth-child(31)")
  webElem$clickElement()
  
  webElem <-
    remDr$findElement("css selector",
                      "#ddlEndHour > option:nth-child(24)")
  webElem$clickElement()
  
  # 地域 ----------------------------------------------------------------------
  select_station <- function(remDr, nth = 1) {
    webElem <-
      remDr$findElement("css selector",
                        glue::glue("#ddlArea > option:nth-child({nth})"))
    # webElem$getElementText()
    webElem$clickElement()
    
    webElem <-
      remDr$findElement("css selector",
                        "#CheckBoxMstList")
    webElem$getElementText()
    webElem <-
      remDr$findElement("css selector",
                        "#btnGetAll")
    webElem$clickElement()
    webElem <-
      remDr$findElement("css selector",
                        "#hyoji")
    # http://kafun.taiki.go.jp/datahyo.aspx に移動
    webElem$clickElement()
    remDr
  }
  
  collect_tbl_data <- function(remDr) {
    
    d <- 
      remDr$getPageSource()[[1]] %>%
      xml2::read_html() %>%
      rvest::html_nodes(css = "#dgd1") %>%
      rvest::html_table(fill = TRUE, header = FALSE) %>%
      purrr::reduce(rbind) %>%
      tibble::as_tibble() %>% 
      dplyr::slice(-seq(1, 2)) %>% 
      purrr::set_names(c("pref_code", "prefecture", "city_code", "city",
                         "station_code", "station",
                         "date", "time",
                         "value",
                         "amedas_wind_direction", "amedas_wind_speed",
                         "amedas_temperature", "amedas_precipitation",
                         "rader")) %>% 
      dplyr::mutate(date = lubridate::as_date(date)) %>% 
      dplyr::mutate(value = dplyr::na_if(value, "欠測") %>% as.numeric()) %>% 
      dplyr::mutate_if(is.character, stringi::stri_trans_nfkc)
    
    remDr$goBack()
    
    d
  }
  
  target_areas <- 
    remDr$getPageSource()[[1]] %>%
    read_html() %>% 
    html_nodes(css = '#ddlArea > option') %>% 
    html_text() %>% 
    stringr::str_subset("地域$") %>% 
    ensure(length(.) == 7L)
  
  # select_station(remDr, 1) %>%
  #   collect_tbl_data()
  # remDr$goBack()
  # remDr$getCurrentUrl()
  
  # ~ 1.5hour
  df_moe2019 <- 
    seq.int(1, length(target_areas)) %>% 
    purrr::map_dfr(~ select_station(remDr, .x) %>% 
                     collect_tbl_data()) %>% 
    verify(dim(.) == c(161499, 14))
  
  df_moe2019 <- 
    df_moe2019 %>% 
    dplyr::mutate(city_code = dplyr::if_else(city_code == "22201",
                                             "22101",
                                             city_code),
                  city = dplyr::if_else(city == "兵庫県加古川市",
                                      "加古川市",
                                      city),
                  #cityがcity_codeになってしまう...
                city = dplyr::if_else(city_code == "27128",
                                     "大阪市中央区",
                                     if_else(city_code == "22101",
                                             "静岡市葵区",
                                             if_else(city_code == "43201",
                                                     "熊本市中央区",
                                                     city)),
                                     city),
                station = dplyr::recode(
                  station,
                  `株式会社江東微生物研究所 微研 東北中央研究所` = "株式会社江東微生物研究所 東北中央研究所"))
  
  invisible(
    df_moe2019 %>% 
      distinct(pref_code, prefecture) %>% 
      verify(nrow(.) == 47L))
  
  invisible(
    df_moe2019 %>% 
      map_dbl(
        ~sum(is.na(.))) %>% 
      unique() %>% 
      ensure(length(.) == 2L))
  invisible(
    df_moe2019 %>% 
      pull(value) %>% 
      is.na() %>% 
      sum() %>% 
      ensure(. == 395L))
  
  # df_moe2019 %>% 
  #   readr::write_csv(here::here("data/japan_archives2019.csv"))
  
  df_moe2019 %>% 
    readr::write_rds(here::here("data/japan_archives2019.rds"), compress = "xz")
  
  # close -------------------------------------------------------------------
  remDr$close()
  rD[["server"]]$stop()
} else {
  df_moe2019 <- 
    readr::read_rds(here::here("data/japan_archives2019.rds"))
}
