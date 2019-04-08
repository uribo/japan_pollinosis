###############################
# docker起動状態で行う
# 以下のコードはローカルで実行
##############################
library(RSelenium)
library(purrr)
library(ensurer)
library(rvest)

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
    read_html() %>%
    html_nodes(css = "#dgd1") %>%
    html_table(fill = TRUE, header = FALSE) %>%
    purrr::reduce(rbind) %>%
    tibble::as_tibble() %>% 
    dplyr::slice(-seq(1, 2)) %>% 
    purrr::set_names(c("pref_code", "prefecture", "city_code", "city",
                       "station_code", "station",
                       "date", "time",
                       "value",
                       "amedas_wind_direction", "amedas_wind_speed",
                       "amedas_temperature", "amedas_precipitation",
                       "rader"))
  
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

df_moe2019 <- 
  seq.int(1, length(target_areas)) %>% 
  purrr::map_dfr(~ select_station(remDr, .x) %>% 
               collect_tbl_data())

df_moe2019 %>% 
  readr::write_csv(here::here("data/japan_archives2019.csv"))

# close -------------------------------------------------------------------
remDr$close()
rD[["server"]]$stop()
