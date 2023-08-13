FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/pipeline-run-flex.py"

COPY . /template

RUN apt-get update \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

ENV PIP_NO_DEPS=True

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]



