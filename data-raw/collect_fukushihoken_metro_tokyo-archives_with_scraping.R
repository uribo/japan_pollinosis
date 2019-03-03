library(purrr)
library(rvest)
library(dplyr)

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
    readr::type_convert()
}

df_pollen_archives <- 
  data_urls %>% 
  purrr::map_dfr(~ slowly(~ parse_table_data(.x,
                                         jp_year = .x %>% 
                                           stringr::str_extract("h[0-9]{2}")), 
                      rate = rate_delay(pause = 3), 
                      quiet = FALSE)(.x)) %>% 
  assertr::verify(dim(.) == c(43992, 6))
