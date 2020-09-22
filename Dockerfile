FROM python:3.8.5-buster

WORKDIR /tmp
RUN apt-get update && apt-get install -y unixodbc unixodbc-dev wget build-essential libc6-dev tdsodbc
RUN wget ftp://ftp.freetds.org/pub/freetds/stable/freetds-1.2.5.tar.gz
RUN tar -xzf freetds-1.2.5.tar.gz

WORKDIR /tmp/freetds-1.2.5
RUN ./configure --prefix=/usr/local --with-tdsver=7.4
RUN make
RUN make install

WORKDIR /srv
RUN rm -rf /tmp/freetds-1.2.5
RUN echo "[FreeTDS]\nDriver = FreeTDS\nDescription = FreeTDS\nTrace = No" >> odbc.ini
RUN echo "[FreeTDS]\nDescription=FreeTDS\nDriver=/usr/local/lib/libtdsodbc.so\nUsageCount=1" >> odbcinst.ini

ENV ODBCSYSINI=/srv

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY plan_monitor ./plan_monitor
ENTRYPOINT ["python", "-m"]
