# Image for the "lab" service: Python + JupyterLab + the packages the notebooks and
# bdv_common.py / bdv_graph.py need. The project folder is mounted at /work at run time,
# so the notebooks, CSV files and result logs stay on the host PC (not inside the image).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /work
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.root_dir=/work"]
