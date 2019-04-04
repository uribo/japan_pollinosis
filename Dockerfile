FROM rocker/geospatial:3.5.3

RUN set -x && \
  apt-get update && \
  apt-get install -y --no-install-recommends \
    unar && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

ARG GITHUB_PAT

RUN set -x && \
  echo "GITHUB_PAT=$GITHUB_PAT" >> /usr/local/lib/R/etc/Renviron

RUN set -x && \
  install2.r --error \
    anytime \
    assertr \
    conflicted \
    ensurer \
    fuzzyjoin \
    gghighlight \
    ggridges \
    jpndistrict \
    tsibble && \
  installGithub.r \
    "abikoushi/ggbrick" \
    "uribo/odkitchen" \
    "tidyverse/dplyr@v0.8.0.1" \
    "tidyverts/fable" && \
  rm -rf /tmp/downloaded_packages/ /tmp/*.rds
