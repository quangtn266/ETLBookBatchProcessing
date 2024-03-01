# ETLBookBatchProcessing
A demo data pipeline is about Flink for Batch processing

# Database generation
1. Because I use Macbook M1 pro, then I need to start postgresql before generating a database.
```
brew services start postgresql
```

2. Generate database "books"
   
```
createdb books  -U postgres
```

3. Generate tables through schema misc/schema.sql

```
psql -h localhost -U quangtn -W -d books -f ../etl_datapipeline/misc/schema.sql 
```

# Getting dataset.

1. Curl the dataset.
```
curl -sL https://github.com/luminati-io/Amazon-popular-books-dataset/raw/main/Amazon_popular_books_dataset.json |   jq -c '.[]' > dataset.json
```

2. Generate a gz dataset, because it load .gz files as data source.
```
gzip dataset.json
```

# Building package.
```
mvn clean package
```

# Running.
```
../flink-1.18.1/bin/flink run -p 4 ./target/github-etl-datapipeline-1.0-SNAPSHOT.jar --input-dir dataset.json ./ --db-url jdbc:postgresql://localhost:5432/books
```

# Output.
![Screen Recording 2024-03-01 at 14 50 43](https://github.com/quangtn266/ETLBookBatchProcessing/assets/50879191/90779f55-632d-4047-b6d9-5794800fe836)
