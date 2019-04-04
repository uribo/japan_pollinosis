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

site_url <- "http://kafun.taiki.go.jp/"

target_page <- 
  str_c(site_url, "library.html") %>% 
  read_html(encoding = "cp932")

if (rlang::is_false(file.exists(here::here("data/japan_archives.csv")))) {
  if (rlang::is_false(dir.exists(here::here("data-raw/moe"))))
    dir.create(here::here("data-raw/moe"))
  
  data_archives <- 
    fs::dir_ls(here::here("data-raw/moe"), 
               recursive = TRUE, 
               regexp = "花粉.+.(xls|xlsx)$")
  
  if (length(data_archives) < 92) {
    
    data_urls <- 
      target_page %>% 
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
                 regexp = "花粉.+.(xls|xlsx)$")
    
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
      d <- 
        readxl::read_excel(input, 
                              skip = 1, 
                              sheet = 1, 
                              na = c("-9998", "-9997", "-9996",
                                     "-9992", "-9991")) %>% 
        dplyr::select(-seq(ncol(.) - 1, ncol(.))) %>% 
        dplyr::mutate_all(as.numeric) %>% 
        mutate(datetime = lubridate::make_datetime(year = `年`, month = `月`, day = `日`, hour = `時`, 
                                                   tz = "Asia/Tokyo")) %>% 
        select(datetime, 5:ncol(.))
      
      d <- 
        d %>% 
        select_if(list(~ sum(is.na(.)) != nrow(d)))
    } 
    
    d %>% 
      filter(!is.na(datetime)) %>% 
      tidyr::gather(station, value, -datetime) %>% 
      mutate(station = stringr::str_remove_all(station, "\t|\n")) %>% 
      mutate_if(is.character, stringi::stri_trans_nfkc)
    
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
      .f = ~ parse_xls_data(.x, year = .y)) %>% 
    reduce(rbind) %>% 
    assertr::verify(dim(.) == c(5212022, 3))
  
  df_pollen_archives_moe %>% 
    filter(is.na(datetime)) %>% 
    assertr::verify(nrow(.) == 0)
  
  df_pollen_archives_moe %>% 
    readr::write_csv(here::here("data/japan_archives.csv"))
} else {
  df_pollen_archives_moe <-
    readr::read_csv(here::here("data/japan_archives.csv"))
}
