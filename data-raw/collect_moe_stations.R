###############################
# 環境省 環境省花粉観測システム
# 測定局
###############################
library(dplyr)
site_url <- "http://kafun.taiki.go.jp/"
target_page <- 
  xml2::read_html(stringr::str_c(site_url, "library.html"),
                  encoding = "cp932")
if (rlang::is_false(file.exists(here::here("data/moe_stations.csv")))) {
  library(rvest)
  library(stringr)
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
      rvest::html_elements(css = table_id) %>% 
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
  df_moe_stations <- 
    df_moe_stations %>% 
    dplyr::mutate(address = dplyr::recode(
      address,
      `千葉県成田市加良部3-3-1` = "成田市加良部3-3-1",
      `徳島県阿南市領家町野神319` = "阿南市領家町野神319",
      `熊本市本荘5-15-18` = "熊本市中央区本荘5-15-18",
    ))
  invisible(
    df_moe_stations %>% 
      filter(stringr::str_detect(note, "名称変更")) %>% 
      assertr::verify(nrow(.) == 2L))
  df_moe_stations %>% 
    readr::write_csv(here::here("data/moe_stations.csv"))
} else {
  df_moe_stations <-
    readr::read_csv(here::here("data/moe_stations.csv"),
                    col_types = c("dccccc"))
}

df_moe_stations_old <- 
  df_moe_stations %>% 
  select(station_id, prefecture, station, note) %>% 
  filter(!is.na(note)) %>% 
  distinct(prefecture, station, note, .keep_all = TRUE) %>% 
  # 元のstationは原則noteに記載される
  tidyr::separate(col = note, into = c("translate_year", "old_station"), sep = "度に")
df_moe_stations_old <- 
  df_moe_stations_old %>% 
  filter(!old_station %in% c("名称変更", "構内移設")) %>% 
  mutate(old_station = stringr::str_remove(old_station, "(から|より)(移転|移設|。)")) %>% 
  bind_rows(
    df_moe_stations_old %>% 
      filter(old_station %in% c("名称変更", "構内移設")) %>% 
      mutate(old_station = case_when(
        prefecture == "長崎県" & old_station == "構内移設" ~ "長崎大学医学部付属病院", 
        prefecture == "長崎県" & old_station == "名称変更" ~ "社団法人全国社会保険協会連合会健康保険諫早総合病院検査部",
        prefecture == "大分県" & old_station == "名称変更" ~ "大分県農林水産研究センター林業試験場"))
  ) %>% 
  arrange(station_id)
df_moe_stations <-
  df_moe_stations %>% 
  select(-note) %>% 
  left_join(df_moe_stations_old, by = c("station_id", "prefecture", "station")) %>% 
  assertr::verify(nrow(.) == 120L) %>% 
  assertr::verify(assertr::has_all_names("station_id", "prefecture", "station",
                                         "address", "amedas_station", "translate_year", "old_station"))
rm(df_moe_stations_old)
