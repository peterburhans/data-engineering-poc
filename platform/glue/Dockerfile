FROM public.ecr.aws/glue/aws-glue-libs:5

RUN python3 -m pip install --no-cache-dir --user "pg8000==1.31.5"

ENV PYTHONPATH="/usr/share/aws/glue-pds/PyGlue.zip:/usr/lib/spark/python/lib:/home/hadoop/workspace/jobs/glue/lib:/home/hadoop/workspace/libs/data_contracts"

WORKDIR /home/hadoop/workspace
