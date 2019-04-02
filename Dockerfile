FROM rocker/tidyverse:3.5.3

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
    assertr \
    conflicted \
    ensurer \
    fuzzyjoin & \
  installGithub.r \
    "uribo/odkitchen" && \
  rm -rf /tmp/downloaded_packages/ /tmp/*.rds
