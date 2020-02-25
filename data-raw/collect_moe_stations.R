###############################
# 環境省 環境省花粉観測システム
# 測定局
###############################
if (rlang::is_false(file.exists(here::here("data/moe_stations.csv")))) {
  library(rvest)
  library(stringr)
  library(dplyr)
  site_url <- "http://kafun.taiki.go.jp/"
  target_page <- 
    str_c(site_url, "library.html") %>% 
    read_html(encoding = "cp932")
  # 観測地点 --------------------------------------------------------------------
  tidyrup_station_list <- function(df) {
    df_gather <- 
      df %>% 
      tibble::as_tibble() %>% 
      dplyr::rename(pref = `都道府県`,
                    station = `設置場所`,
                    address = `所在地`,
                    amedas_station = `最寄のアメダス測定局`,
                    note = `備考`) %>% 
      tidyr::pivot_longer(cols = tidyselect::num_range(prefix = "",
                                                       range = seq.int(2008, 2020)),
                          names_to = "year",
                          values_to = "status")
    df_gather %>% 
      dplyr::mutate(status = dplyr::if_else(stringr::str_detect(status, "\u25cb"), TRUE, FALSE)) %>% 
      fuzzyjoin::stringdist_left_join(jpndistrict::jpnprefs %>%
                                        dplyr::select(prefecture) %>% 
                                        dplyr::mutate(mod_pref = stringr::str_remove(prefecture, "(県|都|府|)$")),
                                      by = c("pref" = "mod_pref"), 
                                      max_dist = 0,
                                      method = "osa") %>% 
      dplyr::select(prefecture, station, address, year, status, 
                    tidyselect::everything(), 
                    -pref, -mod_pref) %>% 
      assertr::verify(nrow(.) == nrow(df_gather)) %>% 
      mutate_if(is.character, na_if, y = "-") %>% 
      mutate_at(vars(station, address, note), 
                .funs = list(~ stringr::str_remove_all(., "\t|\n"))) %>% 
      mutate_if(is.character, stringi::stri_trans_nfkc)
  }
  extract_station_list <- function(target_page, table_id) {
    target_page %>% 
      rvest::html_nodes(css = table_id) %>% 
      rvest::html_table(header = TRUE) %>% 
      purrr::map(
        .x = .,
        .f = ~ tidyrup_station_list(.x))
  }
  # extract_station_list(target_page, "#Table9")
  df_moe_stations <- 
    purrr::map(
      .x = paste0("#Table", seq.int(9, 13)),
      .f = ~ extract_station_list(target_page = target_page, .x)) %>% 
    purrr::flatten_dfr() %>% 
    assertr::verify(dim(.) == c(1560, 7)) %>% 
    distinct(prefecture, station, address, amedas_station, note) %>% 
    assertr::verify(nrow(.) == 120L) %>% 
    mutate(station_id = row_number()) %>% 
    select(station_id, everything())
  df_moe_stations %>% 
    readr::write_csv(here::here("data/moe_stations.csv"))
} else {
  df_moe_stations <-
    readr::read_csv(here::here("data/moe_stations.csv"),
                    col_types = c("dccccc"))
}
