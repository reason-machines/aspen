# Stock `git http-backend` behind Apache, configured as in the
# git-http-backend(1) manual. This is the reference "plain Git over HTTP"
# baseline: one CGI process per request, repositories on local disk.
FROM debian:bookworm-20261027-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends apache2 git ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && a2dismod -f mpm_prefork \
 && a2enmod mpm_event cgid alias env setenvif \
 && mkdir -p /srv/git \
 && chown www-data:www-data /srv/git

COPY git.conf /etc/apache2/conf-enabled/git.conf
COPY new-repo.sh /usr/local/bin/new-repo

EXPOSE 80
CMD ["apache2ctl", "-D", "FOREGROUND"]
