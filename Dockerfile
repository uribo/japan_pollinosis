FROM rocker/geospatial:3.6.2

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
  install2.r --error --skipinstalled --repos 'http://mran.revolutionanalytics.com/snapshot/2020-02-15' \
    renv && \
  rm -rf /tmp/downloaded_packages/ /tmp/*.rds
