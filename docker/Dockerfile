FROM hysds/pge-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Standard Completeness Evaluator"

USER ops

COPY . /home/ops/verdi/ops/standard_product_completeness_evaluator
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/standard_product_completeness_evaluator

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
