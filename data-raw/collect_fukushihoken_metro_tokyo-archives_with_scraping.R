library(purrr)
library(rvest)
library(dplyr)
library(lubridate)

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

parse_table_data <- function(url, jp_year) {
  
  x <- 
    url %>% 
    read_html() %>% 
    html_nodes(css = "#hisan > div > div > div > table.hisanTable") %>% 
    html_table(fill = TRUE) %>% 
    purrr::reduce(rbind) %>% 
    tibble::as_tibble()
  
  x <- 
    x %>% 
    set_names(
      names(x) %>% 
        recode(`日付` = "date", `曜日` = "day"))

  x %>% 
    mutate(date = lubridate::as_date(
      stringr::str_c(odkitchen::convert_jyr(jp_year), 
                     "/", date))) %>% 
    tidyr::gather(station, value, -date, -day) %>% 
    mutate(
      day = factor(day, 
                   levels = readr::locale(date_names = "ja") %>% 
                     purrr::pluck("date_names") %>% 
                     purrr::pluck("day_ab")),
      value = na_if(value, "-"),
      value = na_if(value, "欠測"),
      value = na_if(value, "\u203b")) %>% 
    mutate(type = basename(url) %>% 
             stringr::str_remove(".html"),
           note = value) %>% 
    mutate(note = purrr::pmap_chr(., ~
                                    if_else(identical(readr::guess_parser(..6), "character"),
                                            as.character(..6),
                                            NA_character_))) %>%
    mutate(value = as.character(value)) %>% 
    mutate(
      value = purrr::pmap_dbl(.,
                              ~ if_else(is.na(..6),
                                        as.double(..4),
                                        readr::parse_number(..4)))) %>% 
    readr::type_convert(.,
      col_types = readr::cols(
      station   = readr::col_character(),
      type      = readr::col_character(),
      note      = readr::col_logical()
      )) %>% 
    tidyr::fill(date)
}

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
