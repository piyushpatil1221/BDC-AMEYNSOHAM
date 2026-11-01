# Reuses the embedding-service image (INT8 encoder + tokenizer) and adds plotting.
FROM bdc/embedding:latest
RUN pip install --no-cache-dir matplotlib==3.9.3 PyYAML==6.0.2
WORKDIR /w
ENV PYTHONPATH=/w
ENTRYPOINT ["python", "-m"]
CMD ["eval.offline"]
