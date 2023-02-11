################################
# 東京都福祉保健局 東京都アレルギー情報navi
# 飛散花粉数データ
################################
library(purrr)
library(rvest)
library(dplyr)
library(lubridate)

collect_table_data <- memoise::memoise(
  function(url) {
    check <- 
      stringr::str_detect(url, "archive/(r|h)[0-9]{1,2}")
    if (rlang::is_true(check)) {
      df_sets <- 
        url %>% 
        xml2::read_html() %>% 
        rvest::html_elements(css = "#hisan > div > div > div > table.hisanTable") %>% 
        rvest::html_table(fill = TRUE)
    } else {
      df_sets <- 
        url %>% 
        xml2::read_html() %>% 
        rvest::html_elements(css = '#hisan > div > table') %>% 
        rvest::html_table(header = FALSE) %>% 
        purrr::keep(~ nrow(.) > 1 & ncol(.) == 14)
    }
    if (length(df_sets) > 0) {
      df_sets %>% 
        purrr::map(
          # readr::type_convert(col_types = paste0(rep("c", 14), collapse = ""))
          ~ .x %>% 
            dplyr::mutate(dplyr::across(
              tidyselect::everything(),
              .fns = as.character))
        ) %>% 
        purrr::list_rbind() %>% 
        tibble::as_tibble() %>% 
        purrr::set_names(c("日付", "曜日",
                           as.character(forcats::fct_drop(station_list))))
    }
  }
)

parse_table_data <- function(url, jp_year) {
  x <- 
    collect_table_data(url)
  x <- 
    x %>% 
    purrr::set_names(
      names(x) %>% 
        dplyr::recode(`日付` = "date", `曜日` = "day"))
  x %>% 
    dplyr::mutate(date = 
      stringr::str_c(zipangu::convert_jyear(jp_year), 
                     "/", date)) %>% 
    dplyr::mutate(date = case_when(date == "2009//31/" ~ "2009/3/12",
                                   .default = date)) %>% 
    dplyr::mutate(date = lubridate::as_date(date)) %>% 
    tidyr::pivot_longer(cols = seq.int(3, ncol(.)), 
                        names_to = "station", 
                        values_to = "value") %>% 
    dplyr::mutate(value = as.character(value) %>% 
                    stringr::str_replace("0.\\/", "0.0")) %>% 
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
    dplyr::mutate(station = forcats::fct_relevel(station,
                                          levels(station_list)))
}

site_url <- "https://www.fukushihoken.metro.tokyo.lg.jp/"
station_list <- c("千代田", "葛飾", "杉並", "北", "大田",
                  "青梅", "八王子", "多摩", "町田", "立川", 
                  "府中", "小平") %>% 
  ensurer::ensure(length(.) == 12L) %>% 
  forcats::as_factor() %>% 
  forcats::fct_relevel(c("青梅", "八王子", "立川", "多摩", "町田",
                         "府中", "小平", "杉並", "北", "大田",
                         "千代田", "葛飾"))

# 2023 ----------------------------------------------------------------------
# wip 2023-02-10時点の件数
df_pollen_current <- 
  c("cedar", "japanese_cypress") %>% 
  purrr::map(
    ~ parse_table_data(glue::glue(site_url, 
                                  "allergy/pollen/data/{target}.html", 
                                  target = .x),
                       jp_year = "r5")) %>% 
  purrr::list_rbind() %>% 
  # 2023-01-04~
  filter(date < lubridate::make_date(2023, 4, 1)) %>% 
  assertr::verify(dim(.) == c(888, 6))

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
  ensurer::ensure(length(.) == 36L)

slow_parse_table <- 
  slowly(~ parse_table_data(.x,
                          jp_year = .x %>% 
                            stringr::str_extract("(h|r)[0-9]{1,2}")), 
       rate = rate_delay(pause = 7), 
       quiet = FALSE)

df_pollen_archives <- 
  data_urls %>% 
  purrr::map(~ slow_parse_table(.x)) %>% 
  purrr::list_rbind()

df_pollen_archives %>% 
  filter(is.na(date)) %>% 
  ensurer::ensure(nrow(.) == 0L)

df_pollen_archives %>% 
  count(date, sort = TRUE) %>% 
  filter(n != length(station_list)*2) %>% 
  ensurer::ensure(nrow(.) == 6L)

df_pollen_archives <-
  df_pollen_archives %>% 
  mutate(date = if_else(date == "2005-04-09" & day == "木",
                        ymd("2005-04-07"),
                        date),
         date = if_else(date == "2006-02-07" & day == "金",
                        ymd("2006-02-17"),
                        date),
         date = if_else(date == "2009-03-12" & day == "水",
                        ymd("2009-03-11"),
                        date)) %>% 
  assertr::verify(dim(.) == c(56352, 6))

df_pollen_archives %>% 
  count(date, sort = TRUE) %>% 
  filter(n != length(station_list)*2) %>% 
  ensurer::ensure(nrow(.) == 0L)

# Missing value exist?
df_pollen_archives %>% 
  filter(is.na(value)) %>% 
  assertr::verify(nrow(.) == 84L)

# Complete case?
df_pollen_archives %>% 
  filter_at(vars(date, day, station, type), 
            any_vars(sum(is.na(.)))) %>% 
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
  readr::write_csv(here::here("data/tokyo_archives.csv"))
