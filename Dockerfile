# Docker file for the simpledsapp

FROM fnndsc/ubuntu-python3:latest
MAINTAINER fnndsc "dev@babymri.org"

ENV APPROOT="/usr/src/paralleldsapp"  VERSION="0.1"
COPY ["paralleldsapp", "${APPROOT}"]
COPY ["requirements.txt", "${APPROOT}"]

WORKDIR $APPROOT

RUN pip install -r requirements.txt

CMD ["paralleldsapp.py", "--json"]
