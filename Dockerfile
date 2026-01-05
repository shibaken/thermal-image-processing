# Prepare the base environment.
FROM ghcr.io/dbca-wa/docker-apps-dev:ubuntu_2510_base_python as builder_base_thermal_processing
MAINTAINER asi@dbca.wa.gov.au
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Australia/Perth
ENV PRODUCTION_EMAIL=True
ENV SECRET_KEY="ThisisNotRealKey"
SHELL ["/bin/bash", "-c"]
# Use Australian Mirrors
RUN sed 's/archive.ubuntu.com/au.archive.ubuntu.com/g' /etc/apt/sources.list > /etc/apt/sourcesau.list
RUN mv /etc/apt/sourcesau.list /etc/apt/sources.list
# Use Australian Mirrors

# Key for Build purposes only
ENV FIELD_ENCRYPTION_KEY="Mv12YKHFm4WgTXMqvnoUUMZPpxx1ZnlFkfGzwactcdM="

# Key for Build purposes only
RUN apt-get clean
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install --no-install-recommends -y wget git libmagic-dev gcc binutils libproj-dev python3 python3-setuptools python3-dev python3-pip tzdata libreoffice cron python3-gunicorn
RUN apt-get install --no-install-recommends -y libpq-dev patch virtualenv
RUN apt-get install --no-install-recommends -y postgresql-client mtr 
RUN apt-get install --no-install-recommends -y sqlite3 vim postgresql-client ssh htop iputils-ping python3-azure p7zip-full
RUN apt-get install --no-install-recommends -y software-properties-common 
RUN apt-get install --no-install-recommends -y gdal-bin python3-gdal
RUN apt-get install --no-install-recommends -y libgdal-dev build-essential 

# RUN ln -s /usr/bin/python3 /usr/bin/python 

RUN groupadd -g 5000 oim 
RUN useradd -g 5000 -u 5000 oim -s /bin/bash -d /app
RUN mkdir /app 
RUN chown -R oim.oim /app 

# Default Scripts
# RUN wget https://raw.githubusercontent.com/dbca-wa/wagov_utils/main/wagov_utils/bin/default_script_installer.sh -O /tmp/default_script_installer.sh
# RUN chmod 755 /tmp/default_script_installer.sh
# RUN /tmp/default_script_installer.sh

RUN apt-get install --no-install-recommends -y python3-pil

ENV TZ=Australia/Perth
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY startup.sh /
RUN chmod 755 /startup.sh

# Install Python libs from requirements.txt.
FROM builder_base_thermal_processing as python_libs_thermal_processing
WORKDIR /app
user oim 
RUN virtualenv /app/venv
ENV PATH=/app/venv/bin:$PATH
RUN git config --global --add safe.directory /app

COPY requirements.txt ./
COPY python-cron ./
RUN pip install --upgrade pip 
RUN /app/venv/bin/pip install --no-cache-dir -r requirements.txt 

COPY --chown=oim:oim tipapp tipapp
COPY --chown=oim:oim thermalimageprocessing thermalimageprocessing
COPY --chown=oim:oim manage.py ./

# Install the project (ensure that frontend projects have been built prior to this step).
FROM python_libs_thermal_processing
COPY timezone /etc/timezone
COPY gunicorn.ini ./

COPY .git ./.git

EXPOSE 8080
HEALTHCHECK --interval=1m --timeout=5s --start-period=10s --retries=3 CMD ["wget", "-q", "-O", "-", "http://localhost:8080/"]
CMD ["/startup.sh"]
