/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 20G \
  --num-executors 5 \
  /src/powerservice/trading.py \
  '/tmp/data'
