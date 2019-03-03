library(purrr)
library(rvest)
library(ensurer)

zip_files <- 
  fs::dir_ls(here::here("data-raw"), regexp = ".zip")

if (length(zip_files)  != 28) {

  urls <- "http://www.fukushihoken.metro.tokyo.jp/allergy/pollen/archive/"
  
  data_urls <- 
    urls %>% 
    glue::glue("archive.html") %>% 
    read_html() %>% 
    html_nodes(css = '#contentsBodyKafun > div > ul > li > a') %>% 
    html_attr("href") %>% 
    stringr::str_subset("cedar|japanese_cypress") %>% 
    stringr::str_c(urls, .) %>% 
    ensurer::ensure(length(.) == 28)

  download_zip <- 
    slowly(~ read_html(.x) %>% 
             html_nodes(css = '#hisan > p.csvLink.clearfix > a') %>% 
             html_attr("href") %>% 
             stringr::str_remove("../") %>% 
             stringr::str_c(urls, .) %>% 
             curl::curl_download(destfile = here::here("data-raw", basename(.))), 
           rate = rate_delay(pause = 3), 
           quiet = FALSE)
  
  data_urls %>% 
    purrr::walk(download_zip)
  
  fs::dir_ls(here::here("data-raw"), regexp = "h[0-9]{2}_.+_archive.csv.zip") %>% 
    walk(~ unzip(zipfile = .x, exdir = here::here("data-raw")))
}
