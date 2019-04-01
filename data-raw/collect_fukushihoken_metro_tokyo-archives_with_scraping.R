################################
# 東京都福祉保健局 東京都アレルギー情報navi
# 飛散花粉数データ
################################
library(purrr)
library(rvest)
library(dplyr)
library(lubridate)

collect_table_data <- function(url) {
  
  check <- 
    stringr::str_detect(url, "archive/h[0-9]{2}")
  
  if (rlang::is_true(check)) {
    url %>% 
      xml2::read_html() %>% 
      rvest::html_nodes(css = "#hisan > div > div > div > table.hisanTable") %>% 
      rvest::html_table(fill = TRUE) %>% 
      purrr::reduce(rbind) %>% 
      tibble::as_tibble()
  } else {
    url %>% 
      xml2::read_html() %>% 
      rvest::html_nodes(css = '#hisan > div > table') %>% 
      rvest::html_table(header = TRUE) %>% 
      purrr::keep(~ nrow(.) > 1 & ncol(.) == 14) %>% 
      purrr::reduce(rbind) %>% 
      tibble::as_tibble()
  }
}

parse_table_data <- function(url, jp_year) {
  
  x <- 
    collect_table_data(url)
  
  x <- 
    x %>% 
    purrr::set_names(
      names(x) %>% 
        dplyr::recode(`日付` = "date", `曜日` = "day"))
  
  x %>% 
    dplyr::mutate(date = lubridate::as_date(
      stringr::str_c(odkitchen::convert_jyr(jp_year), 
                     "/", date))) %>% 
    tidyr::gather(station, value, -date, -day) %>% 
    dplyr::mutate(
      day = factor(day, 
                   levels = readr::locale(date_names = "ja") %>% 
                     purrr::pluck("date_names") %>% 
                     purrr::pluck("day_ab")),
      value = dplyr::na_if(value, "-"),
      value = dplyr::na_if(value, "欠測"),
      value = dplyr::na_if(value, "\u203b")) %>% 
    dplyr::mutate(type = basename(url) %>% 
             stringr::str_remove(".html"),
           note = value) %>% 
    dplyr::mutate(note = purrr::pmap_chr(., ~
                                           dplyr::if_else(identical(readr::guess_parser(..6), "character"),
                                            as.character(..6),
                                            NA_character_))) %>%
    dplyr::mutate(value = as.character(value)) %>% 
    dplyr::mutate(
      value = purrr::pmap_dbl(.,
                              ~ dplyr::if_else(is.na(..6),
                                        as.double(..4),
                                        readr::parse_number(..4)))) %>% 
    readr::type_convert(.,
                        col_types = readr::cols(
                          station   = readr::col_character(),
                          type      = readr::col_character(),
                          note      = readr::col_logical()
                        )) %>% 
    tidyr::fill(date) %>% 
    mutate(station = forcats::fct_relevel(station,
                                          "青梅", "八王子", "立川", "多摩", "町田",
                                          "府中", "小平", "杉並", "北", "大田",
                                          "千代田", "葛飾"))
}

site_url <- "http://www.fukushihoken.metro.tokyo.jp/"

# 2019 ----------------------------------------------------------------------
df_pollen_current <- 
  c("cedar", "japanese_cypress") %>% 
  purrr::map_dfr(
    ~ parse_table_data(glue::glue(site_url, "allergy/pollen/data/{target}.html", target = .x),
                       jp_year = "h31")) %>% 
  assertr::verify(dim(.) == c(2088, 6))

# 過去 ----------------------------------------------------------------------
urls <- "allergy/pollen/archive/"
data_urls <- 
  site_url %>%
  glue::glue(urls) %>% 
  glue::glue("archive.html") %>% 
  read_html() %>% 
  html_nodes(css = '#contentsBodyKafun > div > ul > li > a') %>% 
  html_attr("href") %>% 
  stringr::str_subset("cedar|japanese_cypress") %>% 
  stringr::str_c(site_url, urls, .) %>% 
  ensurer::ensure(length(.) == 28)

slow_parse_table <- 
  slowly(~ parse_table_data(.x,
                          jp_year = .x %>% 
                            stringr::str_extract("h[0-9]{2}")), 
       rate = rate_delay(pause = 3), 
       quiet = FALSE)

df_pollen_archives <- 
  data_urls %>% 
  purrr::map_dfr(~ slow_parse_table(.)) %>% 
  mutate(date = if_else(date == "2005-04-09" & day == "木",
                        ymd("2005-04-07"),
                        date),
         date = if_else(date == "2006-02-07" & day == "金",
                        ymd("2006-02-17"),
                        date),
         date = if_else(date == "2009-03-10" & day == "水",
                        ymd("2009-03-11"),
                        date)) %>% 
  assertr::verify(dim(.) == c(43992, 6))

# Missing value exist?
df_pollen_archives %>% 
  filter(is.na(value)) %>% 
  assertr::verify(nrow(.) == 101)

# Complete case?
df_pollen_archives %>% 
  filter_at(vars(date, day, station, type), any_vars(sum(is.na(.)))) %>% 
  assertr::verify(nrow(.) == 0)

# Is unique?
df_pollen_archives %>% 
  count(date, type, station) %>% 
  filter(between(n, 1, 1)) %>% 
  assertr::verify(nrow(.) == nrow(df_pollen_archives))

# merge --------------------------------------------------------------------
df_pollen_current %>%
  bind_rows(df_pollen_archives) %>%
  arrange(type, date, station) %>% 
  readr::write_rds(here::here("data/tokyo_archives.rds"), compress = "xz")
