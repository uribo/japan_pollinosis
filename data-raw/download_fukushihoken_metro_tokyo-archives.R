################################
# 東京都福祉保健局 東京都アレルギー情報navi
# 飛散花粉数データ (過去 2001-2018)
################################
library(purrr)
library(rvest)
library(ensurer)

zip_files <- 
  fs::dir_ls(here::here("data-raw/metro_tokyo"), regexp = ".zip")

if (length(zip_files)  != 28) {
  
  site_url <- "http://www.fukushihoken.metro.tokyo.jp/"

  archive_urls <- 
    glue::glue(site_url, "allergy/pollen/archive/")
  
  data_urls <- 
    archive_urls %>% 
    glue::glue("archive.html") %>% 
    read_html() %>% 
    html_nodes(css = '#contentsBodyKafun > div > ul > li > a') %>% 
    html_attr("href") %>% 
    stringr::str_subset("cedar|japanese_cypress") %>% 
    stringr::str_c(archive_urls, .) %>% 
    ensurer::ensure(length(.) == 28)

  download_zip <- 
    slowly(~ read_html(.x) %>% 
             html_nodes(css = '#hisan > p.csvLink.clearfix > a') %>% 
             html_attr("href") %>% 
             stringr::str_remove("../") %>% 
             stringr::str_c(archive.html, .) %>% 
             curl::curl_download(destfile = here::here("data-raw/metro_tokyo", basename(.))), 
           rate = rate_delay(pause = 3), 
           quiet = FALSE)
  
  data_urls %>% 
    purrr::walk(download_zip)
  
  fs::dir_ls(here::here("data-raw/metro_tokyo"), regexp = "h[0-9]{2}_.+_archive.csv.zip") %>% 
    walk(~ unzip(zipfile = .x, exdir = here::here("data-raw/metro_tokyo")))
}
