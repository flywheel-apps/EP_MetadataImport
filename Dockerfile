FROM python:3.9.2

ENV FLYWHEEL /flywheel/v0
RUN mkdir -p $FLYWHEEL

# Install external dependencies 
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY run.py $FLYWHEEL
COPY manifest.json $FLYWHEEL
COPY load_data.py $FLYWHEEL
COPY import_data.py $FLYWHEEL
COPY flywheel_helpers.py $FLYWHEEL
COPY mapping_class.py $FLYWHEEL


