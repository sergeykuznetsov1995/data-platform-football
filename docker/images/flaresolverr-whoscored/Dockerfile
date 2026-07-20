FROM ghcr.io/flaresolverr/flaresolverr:v3.4.6@sha256:7962759d99d7e125e108e0f5e7f3cdbcd36161776d058d1d9b7153b92ef1af9e

USER root

RUN --network=none install -d -o root -g root -m 0555 \
      /usr/local/libexec/whoscored \
      /usr/local/share/whoscored

COPY --chown=root:root scripts/flaresolverr_extended.py \
  /usr/local/libexec/whoscored/flaresolverr_extended.py
COPY --chown=root:root docker/images/flaresolverr-whoscored/entrypoint.sh \
  /usr/local/bin/whoscored-flaresolverr-entrypoint

RUN --network=none chmod 0444 /usr/local/libexec/whoscored/flaresolverr_extended.py \
    && chmod 0555 /usr/local/bin/whoscored-flaresolverr-entrypoint \
    && mv /app/chromedriver /usr/local/share/whoscored/chromedriver.original \
    && chown root:root /usr/local/share/whoscored/chromedriver.original \
    && chmod 0555 /usr/local/share/whoscored/chromedriver.original \
    && ln -s /tmp/whoscored-chromedriver /app/chromedriver \
    && chmod 0555 /app \
    && sha256sum \
      /usr/local/libexec/whoscored/flaresolverr_extended.py \
      /usr/local/share/whoscored/chromedriver.original \
      > /usr/local/share/whoscored/flaresolverr-extension.sha256 \
    && chmod 0444 /usr/local/share/whoscored/flaresolverr-extension.sha256

USER 1000:1000

CMD ["/usr/local/bin/whoscored-flaresolverr-entrypoint"]
