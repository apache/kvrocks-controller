FROM apache/kvrocks:nightly
USER root
RUN mkdir /tmp/kvrocks7770 /tmp/kvrocks7771

RUN echo "kvrocks -c /var/lib/kvrocks/kvrocks.conf --port 7770 --dir /tmp/kvrocks7770 --daemonize yes --cluster-enabled yes --bind 0.0.0.0" >> start.sh
RUN echo "kvrocks -c /var/lib/kvrocks/kvrocks.conf --port 7771 --dir /tmp/kvrocks7771 --cluster-enabled yes --bind 0.0.0.0" >> start.sh
RUN chmod +x start.sh

EXPOSE 7770:7770
EXPOSE 7771:7771

ENTRYPOINT ["sh","start.sh"]
